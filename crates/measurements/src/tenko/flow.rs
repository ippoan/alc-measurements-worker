//! 自動点呼・遠隔点呼の段 (Refs ippoan/rust-alc-api#680。ippoan/alc-tenko-worker の `sessions.rs` から移した。元は rust の
//! `alc_tenko::tenko_sessions`・repo/tenko_sessions.rs の写し)。path・method・JSON の形・status は今までと同じ
//! (alc-app のキオスク・管理画面・運行管理者席がそのまま使う)。
//!
//! - `GET /tenko/sessions` (`?employee_id=&status=&tenko_type=&tenko_method=&judgment_pending=&date_from=&date_to=&page=&per_page=`)・
//!   `GET /tenko/sessions/{id}` — 一覧 (作成の新しい順) と 1 件
//! - `POST /tenko/sessions/start` (201) — 開始 (予定ありなら予定を消費して自動点呼。予定なしは遠隔点呼)
//! - 段: `PUT …/medical`・`…/self-declaration` (安全判定を続けて行う)・`…/daily-inspection`・`…/carrying-items`・
//!   `…/instruction-confirm`・`…/report`・`…/escalate-remote`、`POST …/cancel`・`…/interrupt`・`…/resume`・`…/self-resume`
//! - アルコールの段は [`super::alcohol`]、運行管理者の判定は [`super::judgment`]
//!
//! 点呼が閉じた段 (完了・中止・中断) では、点呼の記録 (`tenko_records`、不変) を書く ([`record_or_log`]。失敗しても
//! 区切りだけ戻してセッションの更新は残す)。**遠隔点呼は乗務員の側が終わっても閉じない**: 指示の確認・運行報告の
//! 最後の段で判定待ち (`judgment_pending`) にし、記録は運行管理者の判定 ([`super::judgment`]) で書く (判定が記録の hash に入る)。
//!
//! webhook (アルコール検知・運行報告・安全判定の中断・日常点検 NG・手動の中断) は、口の中では送らずに
//! [`PendingFire`] を応答の extension に入れる。worker が応答を返した後に ippoan/alc-tenko-worker へ渡す。
//! **dev端末の要求では渡さない**。
//!
//! rust との違い (alc-tenko-worker の時から):
//! - 口ごとの読み書きを 1 つの transaction にした (rust は文ごとに接続を取り、途中で失敗すると半端な状態が残った)
//! - SQL に tenant_id を明示した (社員の名前・予定の指示・携行品の名前は rust では RLS 任せだった)
//! - 一覧の絞り込みは文を組み立てず、`($n IS NULL OR ...)` の 1 文にした
//! - ログは固定の語だけ (段の名前と kind)。時刻は要求の時計 (`AppState::now`) から取る

use alc_core_wasm::api_error::{bad_request, not_found, ApiError};
use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use const_format::concatcp;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use super::fire::{Event, PendingFire};
use super::record::{employee_name, record_or_log, TenkoSession, SESSION_COLUMNS};
use crate::measurements::{DEFAULT_PAGE, DEFAULT_PER_PAGE, MAX_PER_PAGE};
use crate::{set_device_dev, AppState};

/// 型付きの文の値と型
pub type Arg<'a> = (&'a (dyn ToSql + Sync), Type);

/// escalate-remote の理由の上限 (rust と同じ。超えたら切り詰めずに 400)
pub const MAX_ESCALATE_REASON_LEN: usize = 200;

/// 自動点呼の業務前で血圧が無い提出を弾くときの 400 の文言 (rust と同じ)
pub const BP_REQUIRED_MESSAGE: &str = "systolic and diastolic blood pressure are required (自動点呼では血圧の測定が必須です。血圧計のある端末で測定するか、遠隔点呼に切り替えてください)";

/// 判定待ち (遠隔点呼・IT点呼の乗務員の側が終わった状態。alc-migrations 195)
pub const JUDGMENT_PENDING: &str = "judgment_pending";

/// 点呼の方法 (予定ありは自動点呼、予定なしで始めると遠隔点呼)
pub const AUTO: &str = "自動点呼";
pub const REMOTE: &str = "遠隔点呼";

// --- 行と本文 (rust の alc_tenko::models の写し。JSON の形は同じ) ---

/// rust の `StartTenkoSession`
#[derive(Debug, Clone, Deserialize)]
pub struct StartSession {
    pub schedule_id: Option<Uuid>,
    pub tenko_type: Option<String>,
    pub employee_id: Uuid,
    pub identity_face_photo_url: Option<String>,
    pub location: Option<String>,
}

/// rust の `SubmitMedicalData` (`device_id` は rust でも読むだけで使わない)
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitMedical {
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub medical_manual_input: Option<bool>,
    #[serde(default)]
    pub device_id: Option<Uuid>,
}

/// rust の `SubmitOperationReport` (音声は alc-measurements-worker が置いた URL の文字列だけを受ける)
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitReport {
    pub vehicle_road_status: String,
    pub driver_alternation: String,
    pub vehicle_road_audio_url: Option<String>,
    pub driver_alternation_audio_url: Option<String>,
}

/// rust の `CancelTenkoSession`・`InterruptSession`
#[derive(Debug, Clone, Deserialize)]
pub struct Reason {
    #[serde(default)]
    pub reason: Option<String>,
}

/// rust の `ResumeSession`
#[derive(Debug, Clone, Deserialize)]
pub struct Resume {
    pub reason: String,
}

/// rust の `EscalateToRemote`
#[derive(Debug, Clone, Deserialize)]
pub struct Escalate {
    pub reason: String,
}

/// rust の `SubmitSelfDeclaration`
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitSelfDeclaration {
    pub illness: bool,
    pub fatigue: bool,
    pub sleep_deprivation: bool,
}

/// rust の `SelfDeclaration` (セッションの `self_declaration` の JSON)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfDeclaration {
    pub illness: bool,
    pub fatigue: bool,
    pub sleep_deprivation: bool,
    pub declared_at: DateTime<Utc>,
}

/// rust の `SafetyJudgment`
#[derive(Debug, Clone, Serialize)]
pub struct SafetyJudgment {
    pub status: String,
    pub failed_items: Vec<String>,
    pub judged_at: DateTime<Utc>,
    pub medical_diffs: Option<MedicalDiffs>,
}

/// rust の `MedicalDiffs`
#[derive(Debug, Clone, Default, Serialize)]
pub struct MedicalDiffs {
    pub systolic_diff: Option<i32>,
    pub diastolic_diff: Option<i32>,
    pub temperature_diff: Option<f64>,
}

/// rust の `SubmitDailyInspection`
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitDailyInspection {
    pub brakes: String,
    pub tires: String,
    pub lights: String,
    pub steering: String,
    pub wipers: String,
    pub mirrors: String,
    pub horn: String,
    pub seatbelts: String,
}

/// rust の `CarryingItemCheck`
#[derive(Debug, Clone, Deserialize)]
pub struct CarryingItemCheck {
    pub item_id: Uuid,
    pub checked: bool,
}

/// rust の `SubmitCarryingItemChecks`
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitCarryingItems {
    pub checks: Vec<CarryingItemCheck>,
}

/// rust の `TenkoSessionFilter`
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SessionFilter {
    pub employee_id: Option<Uuid>,
    pub status: Option<String>,
    pub tenko_type: Option<String>,
    pub tenko_method: Option<String>,
    pub judgment_pending: Option<bool>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `TenkoSessionsResponse`
#[derive(Debug, Serialize)]
pub struct SessionsResponse {
    pub sessions: Vec<TenkoSession>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

pub mod sql {
    use super::*;

    /// $1 id / $2 tenant_id
    pub const GET: &str = concatcp!(
        "SELECT ",
        SESSION_COLUMNS,
        " FROM alc_api.tenko_sessions WHERE id = $1 AND tenant_id = $2"
    );

    /// 段を進める前の 1 件 (同じ transaction の中で行を押さえる)
    pub const GET_FOR_UPDATE: &str = concatcp!(GET, " FOR UPDATE");

    /// 一覧の絞り込み。$1 tenant_id / $2 employee_id / $3 status / $4 tenko_type / $5 tenko_method /
    /// $6 judgment_pending (true のときだけ未判定に絞る) / $7 date_from / $8 date_to
    pub const WHERE: &str = "tenant_id = $1 \
         AND ($2::uuid IS NULL OR employee_id = $2) \
         AND ($3::text IS NULL OR status = $3) \
         AND ($4::text IS NULL OR tenko_type = $4) \
         AND ($5::text IS NULL OR tenko_method = $5) \
         AND ($6::bool IS NOT TRUE OR manager_judgment IS NULL) \
         AND ($7::timestamptz IS NULL OR started_at >= $7) \
         AND ($8::timestamptz IS NULL OR started_at <= $8)";

    pub const COUNT: &str = concatcp!("SELECT COUNT(*) FROM alc_api.tenko_sessions WHERE ", WHERE);

    /// $9 件数 / $10 飛ばす件数
    pub const LIST: &str = concatcp!(
        "SELECT ",
        SESSION_COLUMNS,
        " FROM alc_api.tenko_sessions WHERE ",
        WHERE,
        " ORDER BY created_at DESC LIMIT $9 OFFSET $10"
    );

    /// 未消費の予定。$1 id / $2 tenant_id → employee_id, tenko_type, responsible_manager_name
    pub const SCHEDULE_UNCONSUMED: &str =
        "SELECT employee_id, tenko_type, responsible_manager_name \
         FROM alc_api.tenko_schedules WHERE id = $1 AND tenant_id = $2 AND consumed = FALSE";

    /// 予定の消費。$1 id / $2 tenant_id
    pub const CONSUME_SCHEDULE: &str =
        "UPDATE alc_api.tenko_schedules SET consumed = TRUE, updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2";

    /// $1 session_id / $2 schedule_id / $3 tenant_id
    pub const SET_CONSUMED_BY: &str =
        "UPDATE alc_api.tenko_schedules SET consumed_by_session_id = $1 \
         WHERE id = $2 AND tenant_id = $3";

    /// $1 tenant_id / $2 employee_id / $3 schedule_id / $4 tenko_type / $5 tenko_method / $6 status /
    /// $7 identity_face_photo_url / $8 location / $9 responsible_manager_name
    pub const CREATE: &str = concatcp!(
        "INSERT INTO alc_api.tenko_sessions (tenant_id, employee_id, schedule_id, tenko_type, tenko_method, status, \
             identity_verified_at, identity_face_photo_url, location, responsible_manager_name, started_at) \
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8, $9, NOW()) RETURNING ",
        SESSION_COLUMNS
    );

    /// 以下の更新は $(最後の 2 つ) = id / tenant_id。行が無ければ 0 行 (rust は `fetch_one` で 500 だった。下の口は
    /// 直前に同じ transaction で読んでいるので起きない)
    pub const UPDATE_MEDICAL: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = 'self_declaration_pending', \
             temperature = COALESCE($1, temperature), systolic = COALESCE($2, systolic), \
             diastolic = COALESCE($3, diastolic), pulse = COALESCE($4, pulse), \
             medical_measured_at = COALESCE($5, NOW()), medical_manual_input = $6, updated_at = NOW() \
         WHERE id = $7 AND tenant_id = $8 RETURNING ",
        SESSION_COLUMNS
    );

    /// $1 status (完了か判定待ち)。completed_at は完了のときだけ (判定待ちは判定で入る)
    pub const CONFIRM_INSTRUCTION: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, instruction_confirmed_at = NOW(), \
             completed_at = CASE WHEN $1 = 'completed' THEN NOW() END, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    pub const UPDATE_REPORT: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, report_vehicle_road_status = $2, \
             report_driver_alternation = $3, report_vehicle_road_audio_url = $4, \
             report_driver_alternation_audio_url = $5, report_submitted_at = NOW(), completed_at = $6, updated_at = NOW() \
         WHERE id = $7 AND tenant_id = $8 RETURNING ",
        SESSION_COLUMNS
    );

    pub const CANCEL: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = 'cancelled', cancel_reason = $1, completed_at = NOW(), \
             updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    pub const UPDATE_SELF_DECLARATION: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET self_declaration = $1, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    pub const UPDATE_SAFETY_JUDGMENT: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, safety_judgment = $2, \
             interrupted_at = COALESCE($3, interrupted_at), updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5 RETURNING ",
        SESSION_COLUMNS
    );

    pub const UPDATE_DAILY_INSPECTION: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, daily_inspection = $2, \
             cancel_reason = COALESCE($3, cancel_reason), completed_at = COALESCE($4, completed_at), updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6 RETURNING ",
        SESSION_COLUMNS
    );

    pub const UPDATE_CARRYING_ITEMS: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = 'identity_verified', carrying_items_checked = $1, \
             updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    pub const INTERRUPT: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = 'interrupted', interrupted_at = NOW(), \
             cancel_reason = COALESCE($1, cancel_reason), updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    pub const RESUME: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, resumed_at = NOW(), resume_reason = $2, \
             resumed_by_user_id = $3, updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5 RETURNING ",
        SESSION_COLUMNS
    );

    /// 「再開は 1 回まで」を WHERE の `resumed_at IS NULL` で担保する (2 回目は 0 行)
    pub const SELF_RESUME: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET resumed_at = NOW(), resume_reason = $1, resumed_by_user_id = $2, \
             updated_at = NOW() \
         WHERE id = $3 AND tenant_id = $4 AND resumed_at IS NULL RETURNING ",
        SESSION_COLUMNS
    );

    pub const ESCALATE: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET tenko_method = '遠隔点呼', escalated_to_remote_at = NOW(), \
             remote_escalation_reason = $1, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3 RETURNING ",
        SESSION_COLUMNS
    );

    /// $1 tenant_id / $2 employee_id
    pub const BASELINE: &str = concatcp!(
        "SELECT ",
        BASELINE_COLUMNS,
        " FROM alc_api.employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2"
    );

    /// 携行品のマスタの件数。$1 tenant_id
    pub const COUNT_CARRYING_ITEMS: &str =
        "SELECT COUNT(*)::BIGINT FROM alc_api.carrying_items WHERE tenant_id = $1";

    /// 携行品の名前。$1 item_id / $2 tenant_id
    pub const CARRYING_ITEM_NAME: &str =
        "SELECT item_name FROM alc_api.carrying_items WHERE id = $1 AND tenant_id = $2";

    /// $1 session_id / $2 item_id / $3 item_name / $4 checked / $5 checked_at
    pub const UPSERT_CARRYING_CHECK: &str = "INSERT INTO alc_api.tenko_carrying_item_checks \
             (session_id, item_id, item_name, checked, checked_at) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (session_id, item_id) DO UPDATE SET checked = EXCLUDED.checked, checked_at = EXCLUDED.checked_at";
}

// --- 健康の基準値とページ (alc-tenko-worker の models の写しのうち、段が使う分だけ) ---

/// 健康の基準値 (`employee_health_baselines`) のうち、安全判定が使う欄
#[derive(Debug, Clone, PartialEq)]
pub struct EmployeeHealthBaseline {
    pub baseline_systolic: i32,
    pub baseline_diastolic: i32,
    pub baseline_temperature: f64,
    pub systolic_tolerance: i32,
    pub diastolic_tolerance: i32,
    pub temperature_tolerance: f64,
}

/// [`EmployeeHealthBaseline`] の欄の順
pub const BASELINE_COLUMNS: &str = "baseline_systolic, baseline_diastolic, baseline_temperature, \
     systolic_tolerance, diastolic_tolerance, temperature_tolerance";

pub fn baseline(r: &Row) -> EmployeeHealthBaseline {
    EmployeeHealthBaseline {
        baseline_systolic: r.get(0),
        baseline_diastolic: r.get(1),
        baseline_temperature: r.get(2),
        systolic_tolerance: r.get(3),
        diastolic_tolerance: r.get(4),
        temperature_tolerance: r.get(5),
    }
}

/// ページの番号と件数 (測定の一覧と同じ: 件数の既定 50・上限 100、番号は 1 から)。返すのは (page, per_page, 飛ばす件数)
pub fn paging(page: Option<i64>, per_page: Option<i64>) -> (i64, i64, i64) {
    let per_page = per_page.unwrap_or(DEFAULT_PER_PAGE).min(MAX_PER_PAGE);
    let page = page.unwrap_or(DEFAULT_PAGE).max(DEFAULT_PAGE);
    (page, per_page, (page - 1) * per_page)
}

// --- 純粋な規則 (rust と同じ) ---

/// 自己申告で引っかかった項目を足す (JSON が読めなければ何も足さない)
pub fn check_self_declaration(self_declaration: &Option<Value>, failed_items: &mut Vec<String>) {
    let decl = self_declaration
        .as_ref()
        .and_then(|j| serde_json::from_value::<SelfDeclaration>(j.clone()).ok());
    let Some(decl) = decl else { return };
    for (hit, label) in [
        (decl.illness, "illness"),
        (decl.fatigue, "fatigue"),
        (decl.sleep_deprivation, "sleep_deprivation"),
    ] {
        if hit {
            failed_items.push(label.to_owned());
        }
    }
}

/// 基準値との差 (値が無ければ差も無い)。許容を超えたら項目を足す
fn vital_diff<T>(
    val: Option<T>,
    base: T,
    tol: T,
    label: &str,
    failed: &mut Vec<String>,
) -> Option<T>
where
    T: Copy + PartialOrd + std::ops::Sub<Output = T> + std::ops::Neg<Output = T>,
{
    let v = val?;
    let d = v - base;
    if d > tol || -d > tol {
        failed.push(label.to_owned());
    }
    Some(d)
}

/// 安全判定 (rust の `perform_safety_judgment` の計算の部分)。基準値が無ければ体調は見ない
pub fn safety_judgment(
    session: &TenkoSession,
    baseline: Option<&EmployeeHealthBaseline>,
    now: DateTime<Utc>,
) -> SafetyJudgment {
    let mut failed_items = Vec::new();
    let mut diffs = MedicalDiffs::default();
    if let Some(bl) = baseline {
        diffs.systolic_diff = vital_diff(
            session.systolic,
            bl.baseline_systolic,
            bl.systolic_tolerance,
            "systolic",
            &mut failed_items,
        );
        diffs.diastolic_diff = vital_diff(
            session.diastolic,
            bl.baseline_diastolic,
            bl.diastolic_tolerance,
            "diastolic",
            &mut failed_items,
        );
        diffs.temperature_diff = vital_diff(
            session.temperature,
            bl.baseline_temperature,
            bl.temperature_tolerance,
            "temperature",
            &mut failed_items,
        );
    }
    check_self_declaration(&session.self_declaration, &mut failed_items);
    SafetyJudgment {
        status: if failed_items.is_empty() {
            "pass"
        } else {
            "fail"
        }
        .to_owned(),
        failed_items,
        judged_at: now,
        medical_diffs: Some(diffs),
    }
}

/// 乗務員の側がもう進められない status か (rust の `is_terminal_tenko_status` に判定待ちを足した。中断・自己再開で弾く)
pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "cancelled" | "interrupted" | JUDGMENT_PENDING
    )
}

/// 乗務員の側が最後の段 (指示の確認・運行報告) を終えたときの status。遠隔点呼は運行管理者の判定を待ち、ほかは完了
pub fn finished_status(tenko_method: &str) -> &'static str {
    if tenko_method == REMOTE {
        JUDGMENT_PENDING
    } else {
        "completed"
    }
}

/// 管理者の再開先 (rust と同じ。日常点検が無ければ日常点検、自己申告が無ければ自己申告)
pub fn resume_to(session: &TenkoSession) -> &'static str {
    if session.daily_inspection.is_some() && session.self_declaration.is_none() {
        "self_declaration_pending"
    } else {
        "daily_inspection_pending"
    }
}

// --- transaction の中の部品 ---

/// 口の結果 (transaction の外へ持ち出す)
pub enum Flow {
    /// セッションを返す。webhook があれば応答の後に渡す (行は大きいので箱に入れる)
    Session(Box<TenkoSession>, Option<Event>),
    /// 本文の無い status (rust の `StatusCode` だけの失敗)
    Status(StatusCode),
    /// 理由付きの失敗 (rust の `ApiError`。本文は `{"error","message"}` か `{"error"}`)
    Api(ApiError),
}

impl TxOutput for Flow {}

impl From<StatusCode> for Flow {
    fn from(s: StatusCode) -> Self {
        Flow::Status(s)
    }
}

/// 1 件 (行を `FOR UPDATE` で押さえる。同じセッションの段が並んでも、先に読んだ status で 2 回進めない)
pub async fn fetch(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<TenkoSession>, Error> {
    let row = tx
        .query_typed_opt(
            sql::GET_FOR_UPDATE,
            &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
        )
        .await?;
    Ok(row.as_ref().map(TenkoSession::from_row))
}

/// 更新して 1 件を読む (`RETURNING`)
pub async fn one(tx: &TenantTx<'_>, sql: &str, args: &[Arg<'_>]) -> Result<TenkoSession, Error> {
    Ok(TenkoSession::from_row(
        &tx.query_typed_one(sql, args).await?,
    ))
}

/// 予定の指示事項 (予定が無い・指示が無ければ `None`)
async fn instruction(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    schedule_id: Option<Uuid>,
) -> Result<Option<String>, Error> {
    let Some(sid) = schedule_id else {
        return Ok(None);
    };
    Ok(tx
        .query_typed_opt(
            super::record::sql::INSTRUCTION,
            &[(&sid, Type::UUID), (&tenant_id, Type::UUID)],
        )
        .await?
        .and_then(|r| r.get(0)))
}

// --- 口 ---

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/sessions", get(list_handler))
        .route("/tenko/sessions/start", post(start_handler))
        .route("/tenko/sessions/{id}", get(get_handler))
        .route("/tenko/sessions/{id}/medical", put(medical_handler))
        .route(
            "/tenko/sessions/{id}/escalate-remote",
            put(escalate_handler),
        )
        .route(
            "/tenko/sessions/{id}/instruction-confirm",
            put(instruction_handler),
        )
        .route("/tenko/sessions/{id}/report", put(report_handler))
        .route("/tenko/sessions/{id}/cancel", post(cancel_handler))
        .route(
            "/tenko/sessions/{id}/self-declaration",
            put(self_declaration_handler),
        )
        .route(
            "/tenko/sessions/{id}/daily-inspection",
            put(daily_inspection_handler),
        )
        .route(
            "/tenko/sessions/{id}/carrying-items",
            put(carrying_items_handler),
        )
        .route("/tenko/sessions/{id}/interrupt", post(interrupt_handler))
        .route("/tenko/sessions/{id}/resume", post(resume_handler))
        .route(
            "/tenko/sessions/{id}/self-resume",
            post(self_resume_handler),
        )
}

/// 要求の今 (時計から)
pub fn now(state: &AppState) -> DateTime<Utc> {
    (state.now)()
}

/// 1 つの transaction で `f` を流し、結果を応答にする。webhook は dev でなければ応答の extension に入れる ([`PendingFire`])。
/// DB の失敗の本文は rust の口に合わせる: 理由付きの口 (`json_errors`) は `{"error":"internal_error"}`、ほかは空
pub async fn run<F>(
    state: AppState,
    tenant_id: Uuid,
    dev: bool,
    stage: &'static str,
    json_errors: bool,
    f: F,
) -> Response
where
    F: for<'t> FnOnce(&'t TenantTx<'t>) -> futures_util::future::BoxFuture<'t, Result<Flow, Error>>
        + Send
        + 'static,
{
    let flow = {
        let mut pg = state.pg.lock().await;
        pg.tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                set_device_dev(tx, dev).await?;
                f(tx).await
            })
        })
        .await
    };
    match flow {
        Ok(Flow::Session(s, event)) => {
            let fire = PendingFire::new(&state, tenant_id, dev, event.into_iter().collect());
            (fire.map(Extension), Json(s)).into_response()
        }
        Ok(Flow::Status(code)) => code.into_response(),
        Ok(Flow::Api(e)) => e.into_response(),
        Err(e) => {
            let code = state.db_fail(stage, &e);
            if json_errors {
                let body: ApiError = (code, Json(json!({ "error": "internal_error" })));
                body.into_response()
            } else {
                code.into_response()
            }
        }
    }
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<SessionFilter>,
) -> Result<Json<SessionsResponse>, StatusCode> {
    let (page, per_page, offset) = paging(filter.page, filter.per_page);
    let mut pg = state.pg.lock().await;
    let dev = dev.get();
    let (total, sessions) = list(&mut pg, tenant_id, dev, filter, per_page, offset)
        .await
        .map_err(|e| state.db_fail("session list", &e))?;
    Ok(Json(SessionsResponse {
        sessions,
        total,
        page,
        per_page,
    }))
}

/// 一覧 (件数と頁)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    f: SessionFilter,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<TenkoSession>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let pending = f.judgment_pending;
            let args: [Arg; 8] = [
                (&tenant_id, Type::UUID),
                (&f.employee_id, Type::UUID),
                (&f.status, Type::TEXT),
                (&f.tenko_type, Type::TEXT),
                (&f.tenko_method, Type::TEXT),
                (&pending, Type::BOOL),
                (&f.date_from, Type::TIMESTAMPTZ),
                (&f.date_to, Type::TIMESTAMPTZ),
            ];
            let total: i64 = tx.query_typed_one(sql::COUNT, &args).await?.get(0);
            let mut list_args: Vec<Arg> = args.to_vec();
            list_args.push((&limit, Type::INT8));
            list_args.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &list_args).await?;
            Ok((total, rows.iter().map(TenkoSession::from_row).collect()))
        })
    })
    .await
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Response {
    run(
        state,
        tenant_id,
        dev.get(),
        "session get",
        false,
        move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                    .await?;
                Ok(match row.as_ref().map(TenkoSession::from_row) {
                    Some(s) => Flow::Session(Box::new(s), None),
                    None => StatusCode::NOT_FOUND.into(),
                })
            })
        },
    )
    .await
}

async fn start_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<StartSession>,
) -> Response {
    let resp = run(
        state,
        tenant_id,
        dev.get(),
        "session start",
        false,
        move |tx| {
            Box::pin(async move {
                // 予定あり (自動点呼) / なし (遠隔点呼)
                let (tenko_type, method, manager) = match body.schedule_id {
                    Some(sid) => {
                        let row = tx
                            .query_typed_opt(
                                sql::SCHEDULE_UNCONSUMED,
                                &[(&sid, Type::UUID), (&tenant_id, Type::UUID)],
                            )
                            .await?;
                        let Some(row) = row else {
                            return Ok(StatusCode::NOT_FOUND.into());
                        };
                        if row.get::<_, Uuid>(0) != body.employee_id {
                            return Ok(StatusCode::BAD_REQUEST.into());
                        }
                        tx.execute_typed(
                            sql::CONSUME_SCHEDULE,
                            &[(&sid, Type::UUID), (&tenant_id, Type::UUID)],
                        )
                        .await?;
                        (row.get::<_, String>(1), AUTO, Some(row.get::<_, String>(2)))
                    }
                    None => (
                        body.tenko_type
                            .clone()
                            .unwrap_or_else(|| "pre_operation".to_owned()),
                        REMOTE,
                        None,
                    ),
                };
                // 業務前は体温・血圧から始める
                let status = if tenko_type == "pre_operation" {
                    "medical_pending"
                } else {
                    "identity_verified"
                };
                let s = one(
                    tx,
                    sql::CREATE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&body.employee_id, Type::UUID),
                        (&body.schedule_id, Type::UUID),
                        (&tenko_type, Type::TEXT),
                        (&method, Type::TEXT),
                        (&status, Type::TEXT),
                        (&body.identity_face_photo_url, Type::TEXT),
                        (&body.location, Type::TEXT),
                        (&manager, Type::TEXT),
                    ],
                )
                .await?;
                if let Some(sid) = body.schedule_id {
                    tx.execute_typed(
                        sql::SET_CONSUMED_BY,
                        &[
                            (&s.id, Type::UUID),
                            (&sid, Type::UUID),
                            (&tenant_id, Type::UUID),
                        ],
                    )
                    .await?;
                }
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await;
    // rust は作成を 201 で返す
    if resp.status() == StatusCode::OK {
        let (mut parts, body) = resp.into_parts();
        parts.status = StatusCode::CREATED;
        return Response::from_parts(parts, body);
    }
    resp
}

async fn medical_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitMedical>,
) -> Response {
    run(
        state,
        tenant_id,
        dev.get(),
        "session medical",
        true,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(Flow::Api(not_found("session_not_found")));
                };
                if cur.tenko_type != "pre_operation" {
                    return Ok(Flow::Api(bad_request(
                        "invalid_tenko_type",
                        &format!(
                            "medical data is only accepted for pre_operation sessions (got {})",
                            cur.tenko_type
                        ),
                    )));
                }
                if cur.status != "medical_pending" {
                    return Ok(Flow::Api(bad_request(
                        "invalid_session_status",
                        &format!(
                            "session status is {} (expected medical_pending)",
                            cur.status
                        ),
                    )));
                }
                // 自動点呼は血圧 (最高・最低) が必須 (端末の状態による免除は無い)
                if cur.tenko_method == AUTO && (body.systolic.is_none() || body.diastolic.is_none())
                {
                    return Ok(Flow::Api(bad_request("bp_required", BP_REQUIRED_MESSAGE)));
                }
                let s = one(
                    tx,
                    sql::UPDATE_MEDICAL,
                    &[
                        (&body.temperature, Type::FLOAT8),
                        (&body.systolic, Type::INT4),
                        (&body.diastolic, Type::INT4),
                        (&body.pulse, Type::INT4),
                        (&body.medical_measured_at, Type::TIMESTAMPTZ),
                        (&body.medical_manual_input, Type::BOOL),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn escalate_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<Escalate>,
) -> Response {
    let reason = body.reason.trim().to_owned();
    if reason.is_empty() || reason.chars().count() > MAX_ESCALATE_REASON_LEN {
        return StatusCode::BAD_REQUEST.into_response();
    }
    run(
        state,
        tenant_id,
        dev.get(),
        "session escalate",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                // 業務前の自動点呼だけ。終了済みは弾く
                if cur.tenko_type != "pre_operation"
                    || cur.tenko_method != AUTO
                    || matches!(cur.status.as_str(), "completed" | "cancelled")
                {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let s = one(
                    tx,
                    sql::ESCALATE,
                    &[
                        (&reason, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn instruction_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Response {
    let log = state.log.clone();
    run(
        state,
        tenant_id,
        dev.get(),
        "session instruction",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.status != "instruction_pending" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let next = finished_status(&cur.tenko_method);
                let s = one(
                    tx,
                    sql::CONFIRM_INSTRUCTION,
                    &[
                        (&next, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                // 判定待ちは記録を書かない (判定で書く)
                if next == "completed" {
                    record_or_log(tx, &s, &log).await;
                }
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn report_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitReport>,
) -> Response {
    // 両項目とも必須 (テキストか「報告なし」)
    if body.vehicle_road_status.trim().is_empty() || body.driver_alternation.trim().is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let at = now(&state);
    let log = state.log.clone();
    run(
        state,
        tenant_id,
        dev.get(),
        "session report",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.tenko_type != "post_operation" || cur.status != "report_pending" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let pending = instruction(tx, tenant_id, cur.schedule_id).await?.is_some();
                let next = if pending {
                    "instruction_pending"
                } else {
                    finished_status(&cur.tenko_method)
                };
                let completed_at = (next == "completed").then_some(at);
                let s = one(
                    tx,
                    sql::UPDATE_REPORT,
                    &[
                        (&next, Type::TEXT),
                        (&body.vehicle_road_status, Type::TEXT),
                        (&body.driver_alternation, Type::TEXT),
                        (&body.vehicle_road_audio_url, Type::TEXT),
                        (&body.driver_alternation_audio_url, Type::TEXT),
                        (&completed_at, Type::TIMESTAMPTZ),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                if next == "completed" {
                    record_or_log(tx, &s, &log).await;
                }
                let payload = json!({
                    "event": "report_submitted",
                    "timestamp": at,
                    "tenant_id": tenant_id,
                    "data": {
                        "session_id": s.id,
                        "employee_id": s.employee_id,
                        "vehicle_road_status": body.vehicle_road_status,
                        "driver_alternation": body.driver_alternation,
                        "vehicle_road_audio_url": body.vehicle_road_audio_url,
                        "driver_alternation_audio_url": body.driver_alternation_audio_url,
                    }
                });
                Ok(Flow::Session(
                    Box::new(s),
                    Some(Event::new("report_submitted", payload)),
                ))
            })
        },
    )
    .await
}

async fn cancel_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<Reason>,
) -> Response {
    let log = state.log.clone();
    run(
        state,
        tenant_id,
        dev.get(),
        "session cancel",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                // 判定待ちは乗務員の側が終わっている (閉じるのは運行管理者の判定)
                if matches!(
                    cur.status.as_str(),
                    "completed" | "cancelled" | JUDGMENT_PENDING
                ) {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let s = one(
                    tx,
                    sql::CANCEL,
                    &[
                        (&body.reason, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                record_or_log(tx, &s, &log).await;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn self_declaration_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitSelfDeclaration>,
) -> Response {
    let at = now(&state);
    let log = state.log.clone();
    run(
        state,
        tenant_id,
        dev.get(),
        "session self-declaration",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.tenko_type != "pre_operation" || cur.status != "self_declaration_pending" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let declaration = serde_json::to_value(SelfDeclaration {
                    illness: body.illness,
                    fatigue: body.fatigue,
                    sleep_deprivation: body.sleep_deprivation,
                    declared_at: at,
                })
                .expect("自己申告は JSON にできる");
                let s = one(
                    tx,
                    sql::UPDATE_SELF_DECLARATION,
                    &[
                        (&declaration, Type::JSONB),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                // 安全判定を続けて行う
                let bl = tx
                    .query_typed_opt(
                        sql::BASELINE,
                        &[(&tenant_id, Type::UUID), (&s.employee_id, Type::UUID)],
                    )
                    .await?
                    .map(|r| baseline(&r));
                if bl.is_none() {
                    log("measurements: no health baseline, safety judgment defaults to pass");
                }
                let judgment = safety_judgment(&s, bl.as_ref(), at);
                let pass = judgment.status == "pass";
                let next = if pass {
                    "daily_inspection_pending"
                } else {
                    "interrupted"
                };
                let interrupted_at = (!pass).then_some(at);
                let judgment_json = serde_json::to_value(&judgment).expect("判定は JSON にできる");
                let s = one(
                    tx,
                    sql::UPDATE_SAFETY_JUDGMENT,
                    &[
                        (&next, Type::TEXT),
                        (&judgment_json, Type::JSONB),
                        (&interrupted_at, Type::TIMESTAMPTZ),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                if pass {
                    return Ok(Flow::Session(Box::new(s), None));
                }
                record_or_log(tx, &s, &log).await;
                let name = employee_name(tx, tenant_id, s.employee_id).await?;
                let payload = json!({
                    "event": "tenko_interrupted",
                    "timestamp": at,
                    "tenant_id": tenant_id,
                    "data": {
                        "session_id": s.id,
                        "employee_id": s.employee_id,
                        "employee_name": name.unwrap_or_default(),
                        "failed_items": judgment.failed_items,
                        "responsible_manager_name": s.responsible_manager_name,
                        "tenko_type": s.tenko_type,
                    }
                });
                Ok(Flow::Session(
                    Box::new(s),
                    Some(Event::new("tenko_interrupted", payload)),
                ))
            })
        },
    )
    .await
}

async fn daily_inspection_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitDailyInspection>,
) -> Response {
    let items = [
        &body.brakes,
        &body.tires,
        &body.lights,
        &body.steering,
        &body.wipers,
        &body.mirrors,
        &body.horn,
        &body.seatbelts,
    ];
    if items.iter().any(|i| *i != "ok" && *i != "ng") {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let has_ng = items.iter().any(|i| *i == "ng");
    let at = now(&state);
    let inspection = json!({
        "brakes": body.brakes,
        "tires": body.tires,
        "lights": body.lights,
        "steering": body.steering,
        "wipers": body.wipers,
        "mirrors": body.mirrors,
        "horn": body.horn,
        "seatbelts": body.seatbelts,
        "inspected_at": at,
    });
    let log = state.log.clone();
    run(
        state,
        tenant_id,
        dev.get(),
        "session daily-inspection",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.tenko_type != "pre_operation" || cur.status != "daily_inspection_pending" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let next = if has_ng {
                    "cancelled"
                } else {
                    // 携行品のマスタがあれば携行品の確認、なければ本人確認済み
                    let count: i64 = tx
                        .query_typed_one(sql::COUNT_CARRYING_ITEMS, &[(&tenant_id, Type::UUID)])
                        .await?
                        .get(0);
                    if count > 0 {
                        "carrying_items_pending"
                    } else {
                        "identity_verified"
                    }
                };
                let cancel_reason = has_ng.then(|| "日常点検異常".to_owned());
                let completed_at = has_ng.then_some(at);
                let s = one(
                    tx,
                    sql::UPDATE_DAILY_INSPECTION,
                    &[
                        (&next, Type::TEXT),
                        (&inspection, Type::JSONB),
                        (&cancel_reason, Type::TEXT),
                        (&completed_at, Type::TIMESTAMPTZ),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                if !has_ng {
                    return Ok(Flow::Session(Box::new(s), None));
                }
                record_or_log(tx, &s, &log).await;
                let name = employee_name(tx, tenant_id, s.employee_id).await?;
                let payload = json!({
                    "event": "inspection_ng",
                    "timestamp": at,
                    "tenant_id": tenant_id,
                    "data": {
                        "session_id": s.id,
                        "employee_id": s.employee_id,
                        "employee_name": name.unwrap_or_default(),
                        "inspection": inspection,
                        "responsible_manager_name": s.responsible_manager_name,
                    }
                });
                Ok(Flow::Session(
                    Box::new(s),
                    Some(Event::new("inspection_ng", payload)),
                ))
            })
        },
    )
    .await
}

async fn carrying_items_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SubmitCarryingItems>,
) -> Response {
    let at = now(&state);
    run(
        state,
        tenant_id,
        dev.get(),
        "session carrying-items",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.status != "carrying_items_pending" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let mut results = Vec::with_capacity(body.checks.len());
                for check in &body.checks {
                    let name: String = tx
                        .query_typed_opt(
                            sql::CARRYING_ITEM_NAME,
                            &[(&check.item_id, Type::UUID), (&tenant_id, Type::UUID)],
                        )
                        .await?
                        .map(|r| r.get(0))
                        .unwrap_or_default();
                    let checked_at = check.checked.then_some(at);
                    tx.execute_typed(
                        sql::UPSERT_CARRYING_CHECK,
                        &[
                            (&id, Type::UUID),
                            (&check.item_id, Type::UUID),
                            (&name, Type::TEXT),
                            (&check.checked, Type::BOOL),
                            (&checked_at, Type::TIMESTAMPTZ),
                        ],
                    )
                    .await?;
                    results.push(json!({
                        "item_id": check.item_id,
                        "item_name": name,
                        "checked": check.checked,
                    }));
                }
                let carrying = json!({ "items": results, "checked_at": at });
                let s = one(
                    tx,
                    sql::UPDATE_CARRYING_ITEMS,
                    &[
                        (&carrying, Type::JSONB),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn interrupt_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<Reason>,
) -> Response {
    let at = now(&state);
    run(
        state,
        tenant_id,
        dev.get(),
        "session interrupt",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if is_terminal(&cur.status) {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let s = one(
                    tx,
                    sql::INTERRUPT,
                    &[
                        (&body.reason, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                let name = employee_name(tx, tenant_id, s.employee_id).await?;
                let payload = json!({
                    "event": "tenko_interrupted",
                    "timestamp": at,
                    "tenant_id": tenant_id,
                    "data": {
                        "session_id": s.id,
                        "employee_id": s.employee_id,
                        "employee_name": name.unwrap_or_default(),
                        "reason": body.reason,
                        "responsible_manager_name": s.responsible_manager_name,
                        "tenko_type": s.tenko_type,
                    }
                });
                Ok(Flow::Session(
                    Box::new(s),
                    Some(Event::new("tenko_interrupted", payload)),
                ))
            })
        },
    )
    .await
}

async fn resume_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Extension(user): Extension<AuthUser>,
    Path(id): Path<Uuid>,
    Json(body): Json<Resume>,
) -> Response {
    if body.reason.trim().is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    run(
        state,
        tenant_id,
        dev.get(),
        "session resume",
        false,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(StatusCode::NOT_FOUND.into());
                };
                if cur.status != "interrupted" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let to = resume_to(&cur);
                let by = Some(user.user_id);
                let s = one(
                    tx,
                    sql::RESUME,
                    &[
                        (&to, Type::TEXT),
                        (&body.reason, Type::TEXT),
                        (&by, Type::UUID),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

async fn self_resume_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<Resume>,
) -> Response {
    let reason = body.reason.trim().to_owned();
    if reason.is_empty() {
        return bad_request("reason_required", "再開理由は必須です").into_response();
    }
    // キオスク (端末の鍵 = AuthUser 無し) からは NULL のまま (承認した管理者の列なので埋めると嘘になる)
    let by = user.map(|Extension(u)| u.user_id);
    run(
        state,
        tenant_id,
        dev.get(),
        "session self-resume",
        true,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(Flow::Api(not_found("session_not_found")));
                };
                if is_terminal(&cur.status) {
                    return Ok(Flow::Api(bad_request(
                        "session_not_resumable",
                        "この点呼は再開できません。新しく点呼をやり直してください",
                    )));
                }
                let row = tx
                    .query_typed_opt(
                        sql::SELF_RESUME,
                        &[
                            (&reason, Type::TEXT),
                            (&by, Type::UUID),
                            (&id, Type::UUID),
                            (&tenant_id, Type::UUID),
                        ],
                    )
                    .await?;
                Ok(match row {
                    Some(r) => Flow::Session(Box::new(TenkoSession::from_row(&r)), None),
                    None => Flow::Api(bad_request(
                        "already_resumed",
                        "この点呼は既に再開済みです。新しく点呼をやり直してください",
                    )),
                })
            })
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> EmployeeHealthBaseline {
        EmployeeHealthBaseline {
            baseline_systolic: 120,
            baseline_diastolic: 80,
            baseline_temperature: 36.5,
            systolic_tolerance: 10,
            diastolic_tolerance: 10,
            temperature_tolerance: 0.5,
        }
    }

    #[test]
    fn self_declaration_items() {
        let mut f = Vec::new();
        check_self_declaration(&None, &mut f);
        check_self_declaration(&Some(json!({ "something": "else" })), &mut f);
        assert!(f.is_empty());
        let decl = json!({ "illness": true, "fatigue": true, "sleep_deprivation": true, "declared_at": "2026-01-01T00:00:00Z" });
        check_self_declaration(&Some(decl), &mut f);
        assert_eq!(f, vec!["illness", "fatigue", "sleep_deprivation"]);
    }

    #[test]
    fn vital_diff_reports_both_directions() {
        let mut f = Vec::new();
        assert_eq!(vital_diff(Some(135), 120, 10, "systolic", &mut f), Some(15));
        assert_eq!(
            vital_diff(Some(105), 120, 10, "systolic", &mut f),
            Some(-15)
        );
        assert_eq!(vital_diff(Some(125), 120, 10, "systolic", &mut f), Some(5));
        assert_eq!(vital_diff(None, 120, 10, "systolic", &mut f), None);
        assert_eq!(f, vec!["systolic", "systolic"]);
        let b = base();
        assert_eq!(b.baseline_systolic, 120);
    }

    #[test]
    fn terminal_and_resume_to() {
        for s in ["completed", "cancelled", "interrupted", "judgment_pending"] {
            assert!(is_terminal(s));
        }
        for s in ["identity_verified", "medical_pending", "unexpected"] {
            assert!(!is_terminal(s));
        }
    }

    #[test]
    fn remote_waits_for_judgment() {
        assert_eq!(finished_status("遠隔点呼"), "judgment_pending");
        assert_eq!(finished_status("自動点呼"), "completed");
    }

    #[test]
    fn paging_defaults_and_caps() {
        assert_eq!(paging(None, None), (1, 50, 0));
        assert_eq!(paging(Some(0), Some(500)), (1, 100, 0));
        assert_eq!(paging(Some(3), Some(10)), (3, 10, 20));
    }
}
