//! 運行管理者の判定 `POST /tenko/sessions/{id}/judgment` (Refs ippoan/rust-alc-api#680。ippoan/alc-tenko-worker の
//! `judgment_handler` から移した)。
//!
//! 通すのは管理者ログイン (`AuthUser` が入る) か運行管理者の鍵だけ (キオスクの鍵は 401)。判定した人
//! (`judged_by_employee_id`) は同じテナントの在籍の運行管理者 (role に manager か admin。でなければ 403)。
//! dev でない運行管理者の鍵が判定できるのは IT点呼 だけ。確認の方法 (`method`) は IT点呼 にだけ付く (`it` / `in_person`)。
//!
//! - **判定待ち (`judgment_pending`) のセッション**: 判定を書いて閉じ (OK も NG も `completed`。NG で中止にはしない =
//!   ippoan/alc-app#315 のまま)、**そこで点呼の記録を書く** — `record_data` はセッションの JSON なので判定が hash に入る。
//!   記録が書けなければ判定ごと 500 (判定待ちのまま残り、押し直せる)
//! - **判定が記録に入ったセッション**: 409 `already_recorded` (記録は UPDATE できないので押し直せない)
//! - **それ以外** (判定待ちを足す前に閉じた IT点呼の記録など): 今までどおり判定の列だけ書く (押し直し可。記録は先に
//!   書かれていて判定を含まない — 記録の CSV はセッションを join して出す)
//!
//! 血圧の記録が足りず対面点呼へ切り替えたセッションは、会った運行管理者の所属 ([`office_scope`]) で点呼の方法を決める。
//! 点呼の営業所は車両で決まる。手で選んだ所属 (セッションの車両番号を車両マスタ `maintenance_vehicles` の登録番号と照合)
//! があればそれを、無ければ車検証 (`car_inspection` の使用の本拠の位置 + 使用者) を `tenko_office_carins_bases` で営業所に
//! 引いたものを使う (migration 206)。判定した人の
//! 所属営業所 (`tenko_office_managers`) に車両の営業所が入っていれば同じ営業所 = `通常点呼` のまま、どちらも所属が在って
//! 重ならなければ別の営業所・グループ企業 = 告示 266 号 第四条 2 項により `遠隔点呼` とみなす。どちらかが未設定なら
//! 同じ営業所として扱う (画面は `GET /tenko/sessions/{id}/in-person-scope` で判定の前に見せ、未設定を警告する)

use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use alc_worker_db::TenantTx;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use const_format::concatcp;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_postgres::types::Type;
use uuid::Uuid;

use super::flow::{
    beyond_prod_manager, clean, fetch, is_in_person_switched, one, prod_manager_key, run, Arg,
    Flow, JUDGMENT_PENDING, REMOTE,
};
use super::normal::TENKO_METHOD_IT;
use super::record::{write_record, TenkoSession, RETURNING_SESSION};
use crate::AppState;

/// 確認の方法: 通話で確認 (IT点呼 の判定)
pub const JUDGMENT_METHOD_IT: &str = "it";
/// 確認の方法: 本人が来て対面で確認した
pub const JUDGMENT_METHOD_IN_PERSON: &str = "in_person";

/// 対面の範囲: 車両と同じ営業所の運行管理者が対面で確認した (点呼方法は `通常点呼` のまま)
pub const SCOPE_SAME_OFFICE: &str = "same_office";
/// 対面の範囲: 別の営業所・グループ企業の運行管理者が遠隔点呼の設備を使って対面で確認した。
/// 告示 266 号 第四条 2 項により遠隔点呼とみなすので、判定のとき点呼方法を `遠隔点呼` に書き換えて記録する
pub const SCOPE_OTHER_OFFICE: &str = "other_office";
/// 対面の範囲: 車両か運行管理者の所属が未設定で決められない (同じ営業所として `通常点呼` で記録する)
pub const SCOPE_UNKNOWN: &str = "unknown";

/// rust の `RecordManagerJudgment`
#[derive(Debug, Clone, Deserialize)]
pub struct ManagerJudgment {
    pub judgment: String,
    #[serde(default)]
    pub reason: Option<String>,
    pub judged_by_employee_id: Uuid,
    #[serde(default)]
    pub method: Option<String>,
    /// 運行に従事できない (`ng`) とした場合の代替措置 (自己点検表 業務前 二十 タ。`ok` では保存しない)
    #[serde(default)]
    pub alternative_measure: Option<String>,
}

pub mod sql {
    use super::*;

    /// 判定待ちを閉じる。$1 judgment / $2 reason / $3 判定した人 / $4 確認の方法 / $5 id / $6 tenant_id /
    /// $7 代替措置 (無ければ既存の値を保つ) / $8 点呼方法の書き換え (無ければそのまま)
    pub const CLOSE: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'completed', completed_at = NOW(), manager_judgment = $1, \
             manager_judgment_reason = $2, manager_judgment_by = $3, manager_judgment_method = $4, \
             alternative_measure = COALESCE($7, alternative_measure), tenko_method = COALESCE($8, tenko_method), \
             updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6",
        RETURNING_SESSION
    );

    /// 判定待ちを足す前の記録の判定 (列だけ)。確認の方法は省略時に既存の値を保つ (押し直しで OK / NG だけ直せるように)
    pub const MANAGER_JUDGMENT: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET manager_judgment = $1, manager_judgment_reason = $2, \
             manager_judgment_by = $3, manager_judgment_method = COALESCE($4, manager_judgment_method), \
             alternative_measure = COALESCE($7, alternative_measure), updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6",
        RETURNING_SESSION
    );

    /// 判定が点呼の記録に入っているか。$1 session_id / $2 tenant_id
    pub const JUDGED_IN_RECORD: &str = "SELECT EXISTS (SELECT 1 FROM alc_api.tenko_records \
         WHERE session_id = $1 AND tenant_id = $2 AND record_data ->> 'manager_judgment' IS NOT NULL)";

    /// 在籍の社員の役割 (判定した運行管理者の検査)。$1 tenant_id / $2 employee_id
    pub const JUDGE_ROLE: &str =
        "SELECT role FROM alc_api.employees WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL";

    /// 車両の所属営業所・判定する人の所属営業所 (どちらも `[{id, name, company_name}]` の JSON)・車両の所属の出どころ
    /// (`manual` / `carins` / NULL)。車両は手で選んだ所属 (登録番号を空白を除いて照合) を優先し、無ければ車検証の
    /// 使用の本拠の位置 + 使用者の組 (式は migration 206 と同じ) の対応。車検証はセッションの管理番号・車両 ID・登録番号の
    /// どれかで一致する最新の 1 行 (登録番号は `EntryNoCarNo`。`CarNo` は車台番号)。$1 tenant_id / $2 車両番号 / $3 employee_id / $4 車検証の管理番号 / $5 車検証の車両 ID
    pub const OFFICE_SCOPE: &str = "WITH manual AS ( \
             SELECT v.office_id FROM alc_api.maintenance_vehicles v WHERE v.tenant_id = $1 AND v.deleted_at IS NULL \
             AND v.office_id IS NOT NULL \
             AND regexp_replace(v.registration_number, '[[:space:]　]', '', 'g') = regexp_replace($2, '[[:space:]　]', '', 'g')), \
         ci AS ( \
             SELECT btrim(c.\"UseheadqrterChar\") AS use_base, \
                 btrim(COALESCE(NULLIF(btrim(c.\"UsernameHighLevelChar\"), ''), c.\"UsernameLowLevelChar\")) AS user_name \
             FROM alc_api.car_inspection c WHERE c.tenant_id = $1 AND (c.\"ElectCertMgNo\" = $4 OR c.\"CarId\" = $5 \
             OR regexp_replace(c.\"EntryNoCarNo\", '[[:space:]　]', '', 'g') = regexp_replace($2, '[[:space:]　]', '', 'g')) \
             ORDER BY c.created_at DESC LIMIT 1), \
         picked AS ( \
             SELECT office_id, 'manual' AS src FROM manual \
             UNION ALL SELECT b.office_id, 'carins' FROM alc_api.tenko_office_carins_bases b \
             JOIN ci ON b.use_base = ci.use_base AND b.user_name = ci.user_name \
             WHERE b.tenant_id = $1 AND NOT EXISTS (SELECT 1 FROM manual)) \
         SELECT \
         COALESCE((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'name', o.name, 'company_name', o.company_name) \
             ORDER BY o.name, o.id) FROM alc_api.tenko_offices o WHERE o.tenant_id = $1 \
             AND o.id IN (SELECT office_id FROM picked)), '[]'::jsonb), \
         COALESCE((SELECT jsonb_agg(jsonb_build_object('id', o.id, 'name', o.name, 'company_name', o.company_name) \
             ORDER BY o.name, o.id) FROM alc_api.tenko_offices o WHERE o.tenant_id = $1 AND o.id IN ( \
             SELECT m.office_id FROM alc_api.tenko_office_managers m WHERE m.tenant_id = $1 AND m.employee_id = $3)), \
             '[]'::jsonb), \
         (SELECT min(src) FROM picked)";
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/sessions/{id}/judgment", post(judgment_handler))
        .route("/tenko/sessions/{id}/in-person-scope", get(scope_handler))
}

/// 会った運行管理者の所属の照合 ([`office_scope`] の結果)
pub struct OfficeScope {
    /// [`SCOPE_SAME_OFFICE`] / [`SCOPE_OTHER_OFFICE`] / [`SCOPE_UNKNOWN`]
    pub scope: &'static str,
    /// 車両の所属営業所 (`[{id, name, company_name}]`)
    pub vehicle_offices: Value,
    /// 車両の所属の出どころ (`manual` = 手で選んだ所属 / `carins` = 車検証の対応 / None = 未設定)
    pub vehicle_source: Option<String>,
    /// 運行管理者の所属営業所 (同じ形)
    pub judge_offices: Value,
}

/// 2 つの所属から対面の範囲を決める。重なれば同じ営業所、どちらも在って重ならなければ別の営業所、ほかは未設定
pub fn decide_scope(vehicle_offices: &Value, judge_offices: &Value) -> &'static str {
    let ids = |v: &Value| -> Vec<Value> {
        v.as_array()
            .map(|a| a.iter().map(|o| o["id"].clone()).collect())
            .unwrap_or_default()
    };
    let (vehicle, judge) = (ids(vehicle_offices), ids(judge_offices));
    if vehicle.iter().any(|v| judge.contains(v)) {
        SCOPE_SAME_OFFICE
    } else if !vehicle.is_empty() && !judge.is_empty() {
        SCOPE_OTHER_OFFICE
    } else {
        SCOPE_UNKNOWN
    }
}

/// セッションの車両と判定する人の所属営業所を引いて照合する
pub async fn office_scope(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    session: &TenkoSession,
    judge: Uuid,
) -> Result<OfficeScope, tokio_postgres::Error> {
    let row = tx
        .query_typed_one(
            sql::OFFICE_SCOPE,
            &[
                (&tenant_id, Type::UUID),
                (&session.vehicle_number, Type::TEXT),
                (&judge, Type::UUID),
                (&session.carins_cert_no, Type::TEXT),
                (&session.carins_vehicle_id, Type::TEXT),
            ],
        )
        .await?;
    let (vehicle_offices, judge_offices): (Value, Value) = (row.get(0), row.get(1));
    Ok(OfficeScope {
        scope: decide_scope(&vehicle_offices, &judge_offices),
        vehicle_offices,
        vehicle_source: row.get(2),
        judge_offices,
    })
}

#[derive(Debug, Deserialize)]
pub struct ScopeQuery {
    /// 判定する運行管理者 (`judged_by_employee_id` に送る人)
    pub judge: Uuid,
}

/// `GET /tenko/sessions/{id}/in-person-scope?judge=<employee_id>` — 切り替えた対面点呼で、その運行管理者が判定したときの
/// 対面の範囲 `{"scope", "vehicle_number", "vehicle_offices", "vehicle_source", "judge_offices"}`。通すのは判定と同じ (管理者ログインか
/// 運行管理者の鍵)。切り替えた対面点呼でなければ 400、無ければ 404
async fn scope_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(slot): Extension<DeviceDevSlot>,
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Query(q): Query<ScopeQuery>,
) -> Response {
    if user.is_none() && !slot.is_tenko_manager() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let dev = slot.get();
    run(
        state,
        tenant_id,
        dev,
        "session in-person scope",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if !is_in_person_switched(&cur.tenko_method, cur.schedule_id) {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let s = office_scope(tx, tenant_id, &cur, q.judge).await?;
                Ok(Flow::Api((
                    StatusCode::OK,
                    Json(json!({
                        "scope": s.scope,
                        "vehicle_number": cur.vehicle_number,
                        "vehicle_offices": s.vehicle_offices,
                        "vehicle_source": s.vehicle_source,
                        "judge_offices": s.judge_offices,
                    })),
                )))
            })
        },
    )
    .await
}

/// 判定の確認の方法を、そのセッションで受けてよいか (rust の `judgment_method_acceptable`)
pub fn judgment_method_acceptable(session: &TenkoSession, method: Option<&str>) -> bool {
    // 対面点呼へ切り替えたセッションは対面で確認するので、方法は省略か `in_person` だけ
    if is_in_person_switched(&session.tenko_method, session.schedule_id) {
        return method.is_none_or(|m| m == JUDGMENT_METHOD_IN_PERSON);
    }
    let is_it = session.tenko_method == TENKO_METHOD_IT;
    match method {
        Some(m) => is_it && (m == JUDGMENT_METHOD_IT || m == JUDGMENT_METHOD_IN_PERSON),
        None => !is_it || session.manager_judgment_method.is_some(),
    }
}

async fn judgment_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(slot): Extension<DeviceDevSlot>,
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<ManagerJudgment>,
) -> Response {
    // 管理者ログイン (AuthUser が入る) か、運行管理者の鍵だけ通す。キオスクの鍵は弾く
    let admin = user.is_some();
    if !admin && !slot.is_tenko_manager() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body.judgment != "ok" && body.judgment != "ng" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let dev = slot.get();
    let prod_manager = prod_manager_key(&slot, admin);
    run(
        state,
        tenant_id,
        dev,
        "session judgment",
        false,
        move |tx| {
            Box::pin(async move {
                // 判定した人は同じテナントの在籍の運行管理者 (role に manager か admin)
                let role: Option<Vec<String>> = tx
                    .query_typed_opt(
                        sql::JUDGE_ROLE,
                        &[
                            (&tenant_id, Type::UUID),
                            (&body.judged_by_employee_id, Type::UUID),
                        ],
                    )
                    .await?
                    .map(|r| r.get(0));
                let Some(role) = role else {
                    return Ok(StatusCode::FORBIDDEN.into());
                };
                if !role.iter().any(|r| r == "manager" || r == "admin") {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                // dev でない運行管理者の鍵が判定できるのは IT点呼 の記録だけ (記録の列で決める)
                if beyond_prod_manager(prod_manager, &cur) {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
                let method = body.method.as_deref();
                if !judgment_method_acceptable(&cur, method) {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                // 切り替えた対面点呼: 確認の方法は対面に決まり、車両と運行管理者の所属が別の営業所・グループ企業なら
                // 遠隔点呼とみなす (未設定は同じ営業所として通常点呼のまま)
                let switched = is_in_person_switched(&cur.tenko_method, cur.schedule_id);
                let new_method = if switched
                    && office_scope(tx, tenant_id, &cur, body.judged_by_employee_id)
                        .await?
                        .scope
                        == SCOPE_OTHER_OFFICE
                {
                    Some(REMOTE)
                } else {
                    None
                };
                let recorded_method = if switched {
                    Some(JUDGMENT_METHOD_IN_PERSON)
                } else {
                    method
                };
                // 代替措置は従事できない (ng) 判定のときだけ保存する
                let alternative_measure = if body.judgment == "ng" {
                    clean(body.alternative_measure.as_deref())
                } else {
                    None
                };
                let args: [Arg; 7] = [
                    (&body.judgment, Type::TEXT),
                    (&body.reason, Type::TEXT),
                    (&body.judged_by_employee_id, Type::UUID),
                    (&recorded_method, Type::TEXT),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                    (&alternative_measure, Type::TEXT),
                ];
                if cur.status == JUDGMENT_PENDING {
                    // 閉じて記録を書く (記録が書けなければ判定ごと戻す)
                    let [a1, a2, a3, a4, a5, a6, a7] = args;
                    let s = one(
                        tx,
                        sql::CLOSE,
                        &[a1, a2, a3, a4, a5, a6, a7, (&new_method, Type::TEXT)],
                    )
                    .await?;
                    write_record(tx, &s).await?;
                    return Ok(Flow::Session(Box::new(s), None));
                }
                let recorded: bool = tx
                    .query_typed_one(
                        sql::JUDGED_IN_RECORD,
                        &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                    )
                    .await?
                    .get(0);
                if recorded {
                    return Ok(Flow::Api((
                        StatusCode::CONFLICT,
                        Json(json!({ "error": "already_recorded" })),
                    )));
                }
                let s = one(tx, sql::MANAGER_JUDGMENT, &args).await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}
