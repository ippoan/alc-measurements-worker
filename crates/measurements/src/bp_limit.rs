//! 測定台の血圧の回数の制限と、運行管理者が「本人の測定ではない」血圧を外す口 (Refs ippoan/alc-app#676)。
//!
//! 自動点呼の血圧は 1 回に限り測り直せる (2025/10/6 FAQ)。血圧は点呼のキオスクではなく測定台で測り、点呼は
//! 有効時間内の最新の血圧を使う ([`crate::tenko::flow`] の `LATEST_BP`)。そこで測定台の保存
//! (`PUT /measurements/{id}` で点呼の印が無いもの) は、同じ運転者が測定の有効時間内に [`MAX_STAND_BP`] 回
//! (最初 + 測り直し 1 回) を超えて血圧を入れようとしたら 422 で断る (有効時間は血圧の規則。既定は当日中 = 1 日 2 回まで) ([`limit_reached`])。画面は静脈認証が通った直後に
//! 同じ数え方で `GET /measurements/bp-count?employee_id=` を問い ([`bp_count`])、超えていれば測定に進ませない。
//!
//! 測定台で人が入れ替わり (テレコ) 別の人の血圧が自分の分に入ったときは、運行管理者が
//! `POST /measurements/{id}/exclude-bp` でその測定を理由と氏名を付けて外す ([`exclude_bp`])。外した血圧は回数にも
//! 点呼にも入らない。測定は消さずに残す (外した時刻・理由・人が記録になる。alc-migrations 219)。
//! 外す口は管理者ログイン (`AuthUser`) だけ。端末の鍵 (キオスク・測定台・運行管理者席) からは 401。

use alc_core_wasm::api_error::bad_request;
use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::Measurement;
use crate::tenko::flow::clean;
use crate::{set_device_dev, AppState};

/// 測定台で同じ運転者が測定の有効時間内に入れられる血圧の数 (最初 + 測り直し 1 回)
pub const MAX_STAND_BP: i64 = 2;

mod sql {
    use const_format::concatcp;

    use crate::models::MEASUREMENT_COLUMNS;

    /// 数える文の共通の後ろ半分。前に CTE `me (employee_id, skip_id, counts)` を置いて使う
    /// (`skip_id` = 数えない測定 (送り直しの自分)、`counts` = false なら数は 0)。$2 tenant_id / $3 今。
    /// 列は (数, 血圧の有効時間の分 (NULL = 当日中))。血圧の有効時間は基準値の `bp_validity_minutes` (alc-migrations 222)。
    /// NULL・基準値の行が無ければ当日中 (日本時間の同じ日付。今より後の日付も数える)。血圧の時刻は
    /// `COALESCE(bp_measured_at, medical_measured_at, measured_at)` (点呼に写した血圧は測定台の血圧と同じ時刻なので 1 回に数える)。
    /// 数えないもの: `skip_id` の測定、外した血圧
    const COUNT_TAIL: &str = ", v AS (SELECT (SELECT b.bp_validity_minutes FROM employee_health_baselines b, me \
             WHERE b.tenant_id = $2 AND b.employee_id = me.employee_id) AS mins), \
         t AS (SELECT COALESCE(m.bp_measured_at, m.medical_measured_at, m.measured_at) AS at \
           FROM measurements m, me \
          WHERE m.tenant_id = $2 AND m.employee_id = me.employee_id AND m.id IS DISTINCT FROM me.skip_id \
            AND me.counts AND m.systolic IS NOT NULL AND m.bp_excluded_at IS NULL) \
         SELECT (SELECT count(DISTINCT t.at) FROM t \
          WHERE CASE WHEN v.mins IS NULL \
                     THEN (t.at AT TIME ZONE 'Asia/Tokyo')::date >= ($3 AT TIME ZONE 'Asia/Tokyo')::date \
                     ELSE t.at >= $3 - make_interval(mins => v.mins) END), \
         v.mins FROM v";

    /// この測定に血圧を入れると、同じ運転者の有効時間内の血圧が [`super::MAX_STAND_BP`] を超えるか。$1 id。
    /// この測定に既に血圧が入っていれば (送り直し) 数は 0
    pub const COUNT_FOR_MEASUREMENT: &str = concatcp!(
        "WITH me AS (SELECT employee_id, id AS skip_id, systolic IS NULL AS counts FROM measurements \
             WHERE id = $1 AND tenant_id = $2)",
        COUNT_TAIL
    );

    /// 社員の有効時間内の血圧の数 (静脈認証の直後の問い合わせ)。$1 employee_id
    pub const COUNT_FOR_EMPLOYEE: &str = concatcp!(
        "WITH me AS (SELECT $1::uuid AS employee_id, NULL::uuid AS skip_id, true AS counts)",
        COUNT_TAIL
    );

    /// 外す前の値 (`FOR UPDATE`)。列は (最高血圧, 外した時刻)。$1 id / $2 tenant_id
    pub const BEFORE_EXCLUDE: &str =
        "SELECT systolic, bp_excluded_at FROM measurements WHERE id = $1 AND tenant_id = $2 FOR UPDATE";

    /// $1 理由 / $2 外した運行管理者の氏名 / $3 user_id / $4 id / $5 tenant_id
    pub const EXCLUDE: &str = concatcp!(
        "UPDATE measurements SET bp_excluded_at = NOW(), bp_excluded_reason = $1, bp_excluded_by_name = $2, \
             bp_excluded_by_user_id = $3, updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5 RETURNING ",
        MEASUREMENT_COLUMNS
    );
}

struct Counted(i64, Option<i32>);
impl TxOutput for Counted {}

/// 有効時間内の血圧の数と血圧の有効時間 (分。`None` = 当日中)。`sql` は [`sql::COUNT_FOR_MEASUREMENT`] か [`sql::COUNT_FOR_EMPLOYEE`]
/// ($1 はそれぞれ測定 id・社員 id)
async fn count_in_window(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    sql: &'static str,
    key: Uuid,
    now: DateTime<Utc>,
) -> Result<(i64, Option<i32>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_one(
                    sql,
                    &[
                        (&key, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&now, Type::TIMESTAMPTZ),
                    ],
                )
                .await?;
            Ok(Counted(row.get(0), row.get(1)))
        })
    })
    .await
    .map(|Counted(n, mins)| (n, mins))
}

/// 測定 `id` に血圧を入れると制限を超えるか ([`MAX_STAND_BP`])。測定が無ければ false (保存の側が 404 にする)
pub async fn limit_reached(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, Error> {
    count_in_window(pg, tenant_id, dev, sql::COUNT_FOR_MEASUREMENT, id, now)
        .await
        .map(|(n, _)| n >= MAX_STAND_BP)
}

/// `GET /measurements/bp-count` の問い合わせ
#[derive(Debug, Deserialize)]
pub struct BpCountQuery {
    pub employee_id: Uuid,
}

/// `GET /measurements/bp-count?employee_id=` — 社員の有効時間内の測定台の血圧の数 (数え方は [`limit_reached`] と同じ)。
/// 静脈認証が通った直後に画面が問い、`reached` なら測定に進ませない (保存の 422 は保険として残る)。
/// ログインでも端末の鍵でも通る (保存と同じ扱い)。`employee_id` が無い・不正なら 400 (本文なし)
pub async fn bp_count(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    query: Result<Query<BpCountQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(BpCountQuery { employee_id })) = query else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let now = (state.now)();
    let mut pg = state.pg.lock().await;
    match count_in_window(
        &mut pg,
        tenant_id,
        dev.get(),
        sql::COUNT_FOR_EMPLOYEE,
        employee_id,
        now,
    )
    .await
    {
        Err(e) => state.db_fail("measurement bp count", &e).into_response(),
        Ok((count, mins)) => Json(json!({
            "count": count,
            "limit": MAX_STAND_BP,
            "validity": if mins.is_some() { "minutes" } else { "same_day" },
            "validity_minutes": mins,
            "reached": count >= MAX_STAND_BP,
        }))
        .into_response(),
    }
}

/// `POST /measurements/{id}/exclude-bp` の本文
#[derive(Debug, Deserialize)]
pub struct ExcludeBp {
    pub reason: String,
    /// 外した運行管理者の氏名。省略時はログインの利用者の名前 (空ならメール)
    #[serde(default)]
    pub excluded_by_name: Option<String>,
}

enum Excluded {
    NotFound,
    NotBloodPressure,
    Already,
    Done(Box<Measurement>),
}
impl TxOutput for Excluded {}

/// 血圧を「本人の測定ではない」として外す (理由は必須)。外した測定を返す
pub async fn exclude_bp(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<ExcludeBp>,
) -> Response {
    let Some(Extension(user)) = user else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(reason) = clean(Some(&body.reason)) else {
        return bad_request("reason_required", "外す理由は必須です").into_response();
    };
    // kit の復元は name を空で入れるため (ヘッダーに名前が無い)、空ならメール、それも空なら user_id で代える
    // (外した人は必ず残す。CHECK も氏名を必須にしている)
    let by_name = clean(body.excluded_by_name.as_deref())
        .or_else(|| clean(Some(&user.name)))
        .or_else(|| clean(Some(&user.email)))
        .unwrap_or_else(|| user.user_id.to_string());
    let by = user.user_id;
    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let out = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                set_device_dev(tx, dev).await?;
                let Some(before) = tx
                    .query_typed_opt(
                        sql::BEFORE_EXCLUDE,
                        &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                    )
                    .await?
                else {
                    return Ok(Excluded::NotFound);
                };
                if before.get::<_, Option<i32>>(0).is_none() {
                    return Ok(Excluded::NotBloodPressure);
                }
                if before.get::<_, Option<DateTime<Utc>>>(1).is_some() {
                    return Ok(Excluded::Already);
                }
                let row = tx
                    .query_typed_one(
                        sql::EXCLUDE,
                        &[
                            (&reason, Type::TEXT),
                            (&by_name, Type::TEXT),
                            (&by, Type::UUID),
                            (&id, Type::UUID),
                            (&tenant_id, Type::UUID),
                        ],
                    )
                    .await?;
                Ok(Excluded::Done(Box::new(Measurement::from_row(&row))))
            })
        })
        .await;
    match out {
        Err(e) => state.db_fail("measurement exclude-bp", &e).into_response(),
        Ok(Excluded::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "measurement_not_found" })),
        )
            .into_response(),
        Ok(Excluded::NotBloodPressure) => {
            bad_request("not_blood_pressure", "この測定には血圧がありません").into_response()
        }
        Ok(Excluded::Already) => (
            StatusCode::CONFLICT,
            Json(
                json!({ "error": "already_excluded", "message": "この血圧はすでに外してあります" }),
            ),
        )
            .into_response(),
        Ok(Excluded::Done(m)) => Json(*m).into_response(),
    }
}
