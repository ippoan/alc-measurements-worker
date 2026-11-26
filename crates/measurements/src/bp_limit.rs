//! 測定台の血圧の回数の制限と、運行管理者が「本人の測定ではない」血圧を外す口 (Refs ippoan/alc-app#676)。
//!
//! 自動点呼の血圧は 1 回に限り測り直せる (2025/10/6 FAQ)。血圧は点呼のキオスクではなく測定台で測り、点呼は
//! 有効時間内の最新の血圧を使う ([`crate::tenko::flow`] の `LATEST_BP`)。そこで測定台の保存
//! (`PUT /measurements/{id}` で点呼の印が無いもの) は、同じ運転者が測定の有効時間内に [`MAX_STAND_BP`] 回
//! (最初 + 測り直し 1 回) を超えて血圧を入れようとしたら 422 で断る ([`limit_reached`])。
//!
//! 測定台で人が入れ替わり (テレコ) 別の人の血圧が自分の分に入ったときは、運行管理者が
//! `POST /measurements/{id}/exclude-bp` でその測定を理由と氏名を付けて外す ([`exclude_bp`])。外した血圧は回数にも
//! 点呼にも入らない。測定は消さずに残す (外した時刻・理由・人が記録になる。alc-migrations 219)。
//! 外す口は管理者ログイン (`AuthUser`) だけ。端末の鍵 (キオスク・測定台・運行管理者席) からは 401。

use alc_core_wasm::api_error::bad_request;
use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
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
use crate::tenko::flow::{clean, DEFAULT_VALIDITY_MINUTES};
use crate::{set_device_dev, AppState};

/// 測定台で同じ運転者が測定の有効時間内に入れられる血圧の数 (最初 + 測り直し 1 回)
pub const MAX_STAND_BP: i64 = 2;

mod sql {
    use const_format::concatcp;

    use crate::models::MEASUREMENT_COLUMNS;

    /// この測定に血圧を入れると、同じ運転者の有効時間内の血圧が [`super::MAX_STAND_BP`] を超えるか。
    /// $1 id / $2 tenant_id / $3 今 / $4 有効時間の既定 (分。基準値の行が無いとき)。
    /// 数えないもの: この測定 (送り直し)、外した血圧。点呼の測定に写した血圧は測定台の血圧と同じ時刻なので、
    /// 時刻の重なりは 1 回に数える。この測定に既に血圧が入っていれば (送り直し) 数は 0
    pub const COUNT_IN_WINDOW: &str = "WITH me AS (SELECT employee_id, systolic FROM measurements \
             WHERE id = $1 AND tenant_id = $2), \
         v AS (SELECT COALESCE((SELECT b.measurement_validity_minutes FROM employee_health_baselines b, me \
             WHERE b.tenant_id = $2 AND b.employee_id = me.employee_id), $4::int) AS mins) \
         SELECT count(DISTINCT COALESCE(m.medical_measured_at, m.measured_at)) \
           FROM measurements m, me, v \
          WHERE m.tenant_id = $2 AND m.employee_id = me.employee_id AND m.id <> $1 AND me.systolic IS NULL \
            AND m.systolic IS NOT NULL AND m.bp_excluded_at IS NULL \
            AND COALESCE(m.medical_measured_at, m.measured_at) >= $3 - make_interval(mins => v.mins)";

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

struct Reached(bool);
impl TxOutput for Reached {}

/// 測定 `id` に血圧を入れると制限を超えるか ([`MAX_STAND_BP`])。測定が無ければ false (保存の側が 404 にする)
pub async fn limit_reached(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_one(
                    sql::COUNT_IN_WINDOW,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&now, Type::TIMESTAMPTZ),
                        (&DEFAULT_VALIDITY_MINUTES, Type::INT4),
                    ],
                )
                .await?;
            Ok(Reached(row.get::<_, i64>(0) >= MAX_STAND_BP))
        })
    })
    .await
    .map(|Reached(b)| b)
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
