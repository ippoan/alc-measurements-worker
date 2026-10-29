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

use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Json, Router};
use const_format::concatcp;
use serde::Deserialize;
use serde_json::json;
use tokio_postgres::types::Type;
use uuid::Uuid;

use super::flow::{fetch, one, run, Arg, Flow, JUDGMENT_PENDING};
use super::record::{write_record, TenkoSession, SESSION_COLUMNS};
use crate::AppState;

/// 点呼方法 `IT点呼`
pub const TENKO_METHOD_IT: &str = "IT点呼";
/// 確認の方法: 通話で確認 (IT点呼 の判定)
pub const JUDGMENT_METHOD_IT: &str = "it";
/// 確認の方法: 本人が来て対面で確認した
pub const JUDGMENT_METHOD_IN_PERSON: &str = "in_person";

/// rust の `RecordManagerJudgment`
#[derive(Debug, Clone, Deserialize)]
pub struct ManagerJudgment {
    pub judgment: String,
    #[serde(default)]
    pub reason: Option<String>,
    pub judged_by_employee_id: Uuid,
    #[serde(default)]
    pub method: Option<String>,
}

pub mod sql {
    use super::*;

    /// 判定待ちを閉じる。$1 judgment / $2 reason / $3 判定した人 / $4 確認の方法 / $5 id / $6 tenant_id
    pub const CLOSE: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = 'completed', completed_at = NOW(), manager_judgment = $1, \
             manager_judgment_reason = $2, manager_judgment_by = $3, manager_judgment_method = $4, updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6 RETURNING ",
        SESSION_COLUMNS
    );

    /// 判定待ちを足す前の記録の判定 (列だけ)。確認の方法は省略時に既存の値を保つ (押し直しで OK / NG だけ直せるように)
    pub const MANAGER_JUDGMENT: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET manager_judgment = $1, manager_judgment_reason = $2, \
             manager_judgment_by = $3, manager_judgment_method = COALESCE($4, manager_judgment_method), \
             updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6 RETURNING ",
        SESSION_COLUMNS
    );

    /// 判定が点呼の記録に入っているか。$1 session_id / $2 tenant_id
    pub const JUDGED_IN_RECORD: &str = "SELECT EXISTS (SELECT 1 FROM alc_api.tenko_records \
         WHERE session_id = $1 AND tenant_id = $2 AND record_data ->> 'manager_judgment' IS NOT NULL)";

    /// 在籍の社員の役割 (判定した運行管理者の検査)。$1 tenant_id / $2 employee_id
    pub const JUDGE_ROLE: &str =
        "SELECT role FROM alc_api.employees WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL";
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/sessions/{id}/judgment", post(judgment_handler))
}

/// 判定の確認の方法を、そのセッションで受けてよいか (rust の `judgment_method_acceptable`)
pub fn judgment_method_acceptable(session: &TenkoSession, method: Option<&str>) -> bool {
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
                if !admin && !dev && cur.tenko_method != TENKO_METHOD_IT {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
                let method = body.method.as_deref();
                if !judgment_method_acceptable(&cur, method) {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let args: [Arg; 6] = [
                    (&body.judgment, Type::TEXT),
                    (&body.reason, Type::TEXT),
                    (&body.judged_by_employee_id, Type::UUID),
                    (&body.method, Type::TEXT),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                if cur.status == JUDGMENT_PENDING {
                    // 閉じて記録を書く (記録が書けなければ判定ごと戻す)
                    let s = one(tx, sql::CLOSE, &args).await?;
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
