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
//! - 自己点検表の不足を埋める口と規則 (alc-migrations 204): `PUT …/vehicle` (車両番号。車検証の登録番号 / 予定 / 手入力)、
//!   測定の有効時間 (社員の基準値。`PUT …/medical` は過ぎた測定を 400 `measurement_expired`、再開・自己再開・アルコールの段の
//!   直前へ進める段は未測定のアルコールで有効時間を過ぎていれば `medical_pending` へ戻す)、`self-resume` は管理者が再開済みでも
//!   受け (再開の時刻・理由は上書きしない)、`resume` は再開した運行管理者の氏名 (`resumed_by_name`)、`cancel` は代替措置
//! - 血圧は測定台で測る: `PUT …/medical` は本文に血圧が無ければ、有効時間内の最新の血圧を点呼の測定に写す。無ければ
//!   自動点呼は遠隔点呼として続ける (dev 端末は切り替えない)
//! - 取り消し・中断・再開は運行管理者の鍵でも送れる (運行管理者の点呼の一覧から)。範囲は判定と同じ
//!   ([`prod_manager_key`])。再開は管理者ログインか運行管理者の鍵だけ (キオスクの鍵は 401)
//!
//! 点呼が閉じた段 (完了・中止・中断) では、点呼の記録 (`tenko_records`、不変) を書く ([`close_with_record`]。書けなければ
//! 段の更新ごと戻して閉じず、機器故障を残して 500)。**遠隔点呼は乗務員の側が終わっても閉じない**: 指示の確認・運行報告の
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
use axum::http::{HeaderMap, StatusCode};
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
use super::normal::{TENKO_METHOD_IT, TENKO_METHOD_NORMAL};
use super::record::{
    close_with_record, employee_name, TenkoSession, MEASUREMENT_JOIN, RETURNING_SESSION,
    SESSION_COLUMNS,
};
use crate::carins::{lookup_expiry, CarinsLookup};
use crate::measurements::{device_id_from, DEFAULT_PAGE, DEFAULT_PER_PAGE, MAX_PER_PAGE};
use crate::{set_device_dev, AppState};

/// 型付きの文の値と型
pub type Arg<'a> = (&'a (dyn ToSql + Sync), Type);

/// escalate-remote の理由の上限 (rust と同じ。超えたら切り詰めずに 400)
pub const MAX_ESCALATE_REASON_LEN: usize = 200;

/// 判定待ち (遠隔点呼・IT点呼の乗務員の側が終わった状態。alc-migrations 195)
pub const JUDGMENT_PENDING: &str = "judgment_pending";

/// 点呼の方法 (予定ありは自動点呼、予定なしで始めると遠隔点呼)
pub const AUTO: &str = "自動点呼";
pub const REMOTE: &str = "遠隔点呼";

/// 有効な測定台の血圧が無い自動点呼を遠隔点呼へ切り替えた理由 (`remote_escalation_reason`)
pub const STAND_BP_MISSING_REASON: &str = "測定台の血圧が無い (または有効時間を過ぎた)";

/// 業務前の自動点呼に必要な血圧の記録の日数 (東京の日付の異なり数)。満たないと遠隔点呼へ切り替える
pub const BP_HISTORY_DAYS_REQUIRED: i64 = 10;

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

/// rust の `InterruptSession`
#[derive(Debug, Clone, Deserialize)]
pub struct Reason {
    #[serde(default)]
    pub reason: Option<String>,
}

/// rust の `CancelTenkoSession` + 代替措置 (自己点検表 業務前 二十 タ。運行に従事できない場合の措置)
#[derive(Debug, Clone, Deserialize)]
pub struct Cancel {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub alternative_measure: Option<String>,
}

/// rust の `ResumeSession` + 再開を判断した運行管理者の氏名 (業務前 二十 ヨ。`self-resume` は読むだけで使わない)
/// + 自己再開の本人確認 (業務前 十三。`self-resume` だけが読む)
#[derive(Debug, Clone, Deserialize)]
pub struct Resume {
    pub reason: String,
    #[serde(default)]
    pub resumed_by_name: Option<String>,
    #[serde(default)]
    pub identity: Option<ResumeIdentity>,
}

/// キオスクが自己再開の前に行った本人確認: 照合で決まった社員と方法 (`vein` / `face`)
#[derive(Debug, Clone, Deserialize)]
pub struct ResumeIdentity {
    pub employee_id: Uuid,
    pub method: String,
}

/// 自己再開の本人確認として受ける方法 (`tenko_sessions.resume_identity_method` の CHECK と同じ。migration 209)
pub const RESUME_IDENTITY_METHODS: [&str; 2] = ["vein", "face"];

/// `PUT …/vehicle` の本文 (業務前 二十 ハ)。どちらも無ければ 400
#[derive(Debug, Clone, Deserialize)]
pub struct SetVehicle {
    #[serde(default)]
    pub carins_cert_no: Option<String>,
    #[serde(default)]
    pub vehicle_number: Option<String>,
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
        " FROM alc_api.tenko_sessions s",
        MEASUREMENT_JOIN,
        " WHERE s.id = $1 AND s.tenant_id = $2"
    );

    /// 段を進める前の 1 件 (同じ transaction の中で行を押さえる。押さえるのはセッションだけ — 外部結合の測定の側は
    /// `FOR UPDATE` を掛けられない)
    pub const GET_FOR_UPDATE: &str = concatcp!(GET, " FOR UPDATE OF s");

    /// 一覧の絞り込み。$1 tenant_id / $2 employee_id / $3 status / $4 tenko_type / $5 tenko_method /
    /// $6 judgment_pending (true のときだけ未判定に絞る) / $7 date_from / $8 date_to。セッションの別名は `s`
    pub const WHERE: &str = "s.tenant_id = $1 \
         AND ($2::uuid IS NULL OR s.employee_id = $2) \
         AND ($3::text IS NULL OR s.status = $3) \
         AND ($4::text IS NULL OR s.tenko_type = $4) \
         AND ($5::text IS NULL OR s.tenko_method = $5) \
         AND ($6::bool IS NOT TRUE OR s.manager_judgment IS NULL) \
         AND ($7::timestamptz IS NULL OR s.started_at >= $7) \
         AND ($8::timestamptz IS NULL OR s.started_at <= $8)";

    pub const COUNT: &str = concatcp!(
        "SELECT COUNT(*) FROM alc_api.tenko_sessions s WHERE ",
        WHERE
    );

    /// $9 件数 / $10 飛ばす件数
    pub const LIST: &str = concatcp!(
        "SELECT ",
        SESSION_COLUMNS,
        " FROM alc_api.tenko_sessions s",
        MEASUREMENT_JOIN,
        " WHERE ",
        WHERE,
        " ORDER BY s.created_at DESC LIMIT $9 OFFSET $10"
    );

    /// 未消費の予定。$1 id / $2 tenant_id → employee_id, tenko_type, responsible_manager_name, vehicle_number
    pub const SCHEDULE_UNCONSUMED: &str =
        "SELECT employee_id, tenko_type, responsible_manager_name, vehicle_number \
         FROM alc_api.tenko_schedules WHERE id = $1 AND tenant_id = $2 AND consumed = FALSE";

    /// 予定の消費。$1 id / $2 tenant_id
    pub const CONSUME_SCHEDULE: &str =
        "UPDATE alc_api.tenko_schedules SET consumed = TRUE, updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2";

    /// $1 session_id / $2 schedule_id / $3 tenant_id
    pub const SET_CONSUMED_BY: &str =
        "UPDATE alc_api.tenko_schedules SET consumed_by_session_id = $1 \
         WHERE id = $2 AND tenant_id = $3";

    /// 点呼の値の置き場の測定 (`POST /measurements/start` と同じ started の行)。$1 tenant_id / $2 employee_id /
    /// $3 device_id → id
    pub const START_MEASUREMENT: &str =
        "INSERT INTO alc_api.measurements (tenant_id, employee_id, status, device_id) \
         VALUES ($1, $2, 'started', $3) RETURNING id";

    /// $1 tenant_id / $2 employee_id / $3 schedule_id / $4 tenko_type / $5 tenko_method / $6 status /
    /// $7 identity_face_photo_url / $8 location / $9 responsible_manager_name / $10 measurement_id /
    /// $11 vehicle_number (予定の値) / $12 vehicle_number_source / $13 device_id (`X-Device-Id`)。
    /// 点呼を受けた場所 (location): 端末が送ってきた値 (GPS など。空白だけは無いとみなす) を優先し、無ければ端末が
    /// 所属する営業所の「名称 (所在地)」([`super::super::record::sql::device_office_location`])。所属が無ければ NULL
    pub const CREATE: &str = concatcp!(
        "WITH s AS (INSERT INTO alc_api.tenko_sessions (tenant_id, employee_id, schedule_id, tenko_type, tenko_method, status, \
             identity_verified_at, identity_face_photo_url, location, responsible_manager_name, started_at, \
             measurement_id, vehicle_number, vehicle_number_source) \
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, \
             CASE WHEN btrim($8) <> '' THEN $8 ELSE ",
        super::super::record::sql::device_office_location!("$1", "$13"),
        " END, \
             $9, NOW(), $10, $11, $12)",
        RETURNING_SESSION
    );

    /// 以下の更新は $(最後の 2 つ) = id / tenant_id。行が無ければ 0 行 (rust は `fetch_one` で 500 だった。下の口は
    /// 直前に同じ transaction で読んでいるので起きない)。体温・血圧の段はセッションの段だけ進める (値は測定)
    pub const UPDATE_MEDICAL: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'self_declaration_pending', updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2",
        RETURNING_SESSION
    );

    /// 体温・血圧の段の値を値の置き場の測定に書く (来た値だけ変え、時刻は無ければ NOW())。セッションを更新する前に流し、
    /// セッションの応答は測定から読む)。$1 temperature / $2 systolic / $3 diastolic / $4 pulse / $5 medical_measured_at /
    /// $6 medical_manual_input / $7 measurement_id / $8 tenant_id
    pub const MEDICAL_TO_MEASUREMENT: &str =
        "UPDATE alc_api.measurements SET temperature = COALESCE($1, temperature), systolic = COALESCE($2, systolic), \
             diastolic = COALESCE($3, diastolic), pulse = COALESCE($4, pulse), \
             medical_measured_at = COALESCE($5, NOW()), medical_manual_input = $6, updated_at = NOW() \
         WHERE id = $7 AND tenant_id = $8";

    /// 測定を持たないセッション (段階 2 より前に始まったもの) を測定に結ぶ。$1 measurement_id / $2 id / $3 tenant_id
    pub const SET_MEASUREMENT: &str = "UPDATE alc_api.tenko_sessions SET measurement_id = $1 \
         WHERE id = $2 AND tenant_id = $3 AND measurement_id IS NULL";

    /// $1 status (完了か判定待ち)。completed_at は完了のときだけ (判定待ちは判定で入る)
    pub const CONFIRM_INSTRUCTION: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = $1, instruction_confirmed_at = NOW(), \
             completed_at = CASE WHEN $1 = 'completed' THEN NOW() END, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    pub const UPDATE_REPORT: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = $1, report_vehicle_road_status = $2, \
             report_driver_alternation = $3, report_vehicle_road_audio_url = $4, \
             report_driver_alternation_audio_url = $5, report_submitted_at = NOW(), completed_at = $6, updated_at = NOW() \
         WHERE id = $7 AND tenant_id = $8",
        RETURNING_SESSION
    );

    /// $1 reason / $2 alternative_measure (無ければ既存の値を保つ) / $3 id / $4 tenant_id
    pub const CANCEL: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'cancelled', cancel_reason = $1, completed_at = NOW(), \
             alternative_measure = COALESCE($2, alternative_measure), updated_at = NOW() \
         WHERE id = $3 AND tenant_id = $4",
        RETURNING_SESSION
    );

    pub const UPDATE_SELF_DECLARATION: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET self_declaration = $1, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    pub const UPDATE_SAFETY_JUDGMENT: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = $1, safety_judgment = $2, \
             interrupted_at = COALESCE($3, interrupted_at), updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5",
        RETURNING_SESSION
    );

    pub const UPDATE_DAILY_INSPECTION: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = $1, daily_inspection = $2, \
             cancel_reason = COALESCE($3, cancel_reason), completed_at = COALESCE($4, completed_at), updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6",
        RETURNING_SESSION
    );

    pub const UPDATE_CARRYING_ITEMS: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'identity_verified', carrying_items_checked = $1, \
             updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    pub const INTERRUPT: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'interrupted', interrupted_at = NOW(), \
             cancel_reason = COALESCE($1, cancel_reason), updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    /// $1 status / $2 reason / $3 再開した管理者の user_id / $4 再開した運行管理者の氏名 / $5 id / $6 tenant_id
    pub const RESUME: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = $1, resumed_at = NOW(), resume_reason = $2, \
             resumed_by_user_id = $3, resumed_by_name = $4, updated_at = NOW() \
         WHERE id = $5 AND tenant_id = $6",
        RETURNING_SESSION
    );

    /// 管理者が再開済みのセッションも受ける: 再開の時刻・理由・管理者は既にあれば上書きしない
    /// (業務前 十三)。本人確認の方法は今回のものを残す。$1 reason / $2 user_id / $3 本人確認の方法 / $4 id / $5 tenant_id
    pub const SELF_RESUME: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET resumed_at = COALESCE(resumed_at, NOW()), \
             resume_reason = COALESCE(resume_reason, $1), resumed_by_user_id = COALESCE(resumed_by_user_id, $2), \
             resume_identity_method = $3, updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5",
        RETURNING_SESSION
    );

    /// 血圧の記録が足りない業務前の自動点呼を対面点呼 (`通常点呼`) へ切り替える。$1 理由 / $2 id / $3 tenant_id。
    /// 遠隔点呼ではないので `escalated_to_remote_at` は書かない。予定 (`schedule_id`) が付いたまま `通常点呼` なのが
    /// 切り替えの印 ([`is_in_person_switched`])。理由は `remote_escalation_reason` (理由の列はこれだけ)
    pub const SWITCH_IN_PERSON: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET tenko_method = '通常点呼', \
             remote_escalation_reason = $1, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    pub const ESCALATE: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET tenko_method = '遠隔点呼', escalated_to_remote_at = NOW(), \
             remote_escalation_reason = $1, updated_at = NOW() \
         WHERE id = $2 AND tenant_id = $3",
        RETURNING_SESSION
    );

    /// 血圧の記録がある日数 (東京の日付で数え、同じ日の複数件は 1 日)。$1 tenant_id / $2 employee_id → 日数。
    /// dev 軸は RLS が他の問い合わせと同じく効かせる
    pub const BP_HISTORY_DAYS: &str = "SELECT COUNT(DISTINCT ((COALESCE(m.medical_measured_at, m.measured_at) \
         AT TIME ZONE 'Asia/Tokyo')::date)) FROM alc_api.measurements m \
         WHERE m.tenant_id = $1 AND m.employee_id = $2 AND m.systolic IS NOT NULL AND m.diastolic IS NOT NULL";

    /// 乗務員の最新の血圧 (測定台で測った測定。点呼の測定に写した値も同じ値と時刻なので区別しない)。
    /// $1 tenant_id / $2 employee_id → systolic / diastolic / pulse / 測定の時刻
    pub const LATEST_BP: &str = "SELECT m.systolic, m.diastolic, m.pulse, \
             COALESCE(m.medical_measured_at, m.measured_at) AS at FROM alc_api.measurements m \
         WHERE m.tenant_id = $1 AND m.employee_id = $2 AND m.systolic IS NOT NULL AND m.diastolic IS NOT NULL \
         ORDER BY at DESC LIMIT 1";

    /// $1 tenant_id / $2 employee_id
    pub const BASELINE: &str = concatcp!(
        "SELECT ",
        BASELINE_COLUMNS,
        " FROM alc_api.employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2"
    );

    /// 測定の有効時間 (分。業務前 八)。$1 tenant_id / $2 employee_id。行が無ければ [`DEFAULT_VALIDITY_MINUTES`]
    pub const VALIDITY_MINUTES: &str =
        "SELECT measurement_validity_minutes FROM alc_api.employee_health_baselines \
         WHERE tenant_id = $1 AND employee_id = $2";

    /// 有効時間を過ぎた体温・血圧を測り直させる。$1 id / $2 tenant_id
    pub const BACK_TO_MEDICAL: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET status = 'medical_pending', updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2",
        RETURNING_SESSION
    );

    /// 予定の車両番号。$1 schedule_id / $2 tenant_id
    pub const SCHEDULE_VEHICLE: &str =
        "SELECT vehicle_number FROM alc_api.tenko_schedules WHERE id = $1 AND tenant_id = $2";

    /// 車両番号の記録。$1 vehicle_number / $2 source / $3 carins_cert_no / $4 carins_expires_on / $5 carins_matched_by
    /// ($3〜$5 は車検証に一致したときだけ値を入れ、無ければ既存の値を保つ) / $6 id / $7 tenant_id
    pub const SET_VEHICLE: &str = concatcp!(
        "WITH s AS (UPDATE alc_api.tenko_sessions SET vehicle_number = $1, vehicle_number_source = $2, \
             carins_cert_no = COALESCE($3, carins_cert_no), carins_expires_on = COALESCE($4, carins_expires_on), \
             carins_matched_by = COALESCE($5, carins_matched_by), updated_at = NOW() \
         WHERE id = $6 AND tenant_id = $7",
        RETURNING_SESSION
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

/// 測定の有効時間の既定 (分。基準値の行が無い社員。`employee_health_baselines.measurement_validity_minutes` の DEFAULT と同じ)
pub const DEFAULT_VALIDITY_MINUTES: i32 = 30;

/// 測定台の血圧 ([`sql::LATEST_BP`] の行)
pub struct StandBp {
    pub systolic: i32,
    pub diastolic: i32,
    pub pulse: Option<i32>,
    pub measured_at: DateTime<Utc>,
}

pub fn stand_bp(r: &Row) -> StandBp {
    StandBp {
        systolic: r.get(0),
        diastolic: r.get(1),
        pulse: r.get(2),
        measured_at: r.get(3),
    }
}

/// 体温・血圧の測定が有効時間を過ぎているか
pub fn medical_expired(
    measured_at: DateTime<Utc>,
    validity_minutes: i32,
    now: DateTime<Utc>,
) -> bool {
    measured_at < now - chrono::Duration::minutes(i64::from(validity_minutes))
}

/// 空・空白だけの入力は無いものとして扱い、前後の空白を落とす
pub fn clean(v: Option<&str>) -> Option<String> {
    v.map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
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

/// 血圧の記録が足りず、予定から始めた自動点呼が対面点呼 (`通常点呼`) へ切り替わったセッションか。
/// 測定から作る通常点呼 ([`super::normal`]) は予定を指さない (`schedule_id` が NULL) ので、予定が付いた `通常点呼` で区別できる。
/// 乗務員の側が終わったら判定待ちになり、運行管理者が対面で確認して判定する
pub fn is_in_person_switched(tenko_method: &str, schedule_id: Option<Uuid>) -> bool {
    tenko_method == TENKO_METHOD_NORMAL && schedule_id.is_some()
}

/// 乗務員の側が最後の段 (指示の確認・運行報告) を終えたときの status。遠隔点呼は運行管理者の判定を待ち、ほかは完了
pub fn finished_status(tenko_method: &str, schedule_id: Option<Uuid>) -> &'static str {
    if tenko_method == REMOTE || is_in_person_switched(tenko_method, schedule_id) {
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

/// 本番の (dev でない) 運行管理者の鍵の要求か (管理者ログインは除く)。この鍵が触れる点呼は IT点呼 だけで、
/// 開発用の席の鍵はどの点呼にも触れる (判定・取り消し・中断・再開で同じ範囲。2026-11-02 ユーザー決定)。
/// キオスクの鍵はここに当たらない (乗務員が自分の点呼をやめる口は今までどおり)
pub fn prod_manager_key(slot: &DeviceDevSlot, admin: bool) -> bool {
    !admin && slot.is_tenko_manager() && !slot.get()
}

/// [`prod_manager_key`] の要求が触れない点呼か (403)
pub fn beyond_prod_manager(prod_manager: bool, session: &TenkoSession) -> bool {
    prod_manager
        && session.tenko_method != TENKO_METHOD_IT
        && !is_in_person_switched(&session.tenko_method, session.schedule_id)
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

/// 社員の測定の有効時間 (分)。基準値の行が無ければ [`DEFAULT_VALIDITY_MINUTES`]
pub async fn validity_minutes(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<i32, Error> {
    Ok(tx
        .query_typed_opt(
            sql::VALIDITY_MINUTES,
            &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
        )
        .await?
        .map_or(DEFAULT_VALIDITY_MINUTES, |r| r.get(0)))
}

/// アルコール未測定で、記録済みの体温・血圧の測定が有効時間を過ぎていれば、状態を `medical_pending` へ戻す
/// (業務前 八。体温・血圧の測り直しから。自己申告と安全判定もやり直し)。再開と、アルコールの段の直前へ進める段が使う。
/// 戻さないときは受けたセッションをそのまま返す
pub async fn back_to_medical_if_expired(
    tx: &TenantTx<'_>,
    s: TenkoSession,
    now: DateTime<Utc>,
) -> Result<TenkoSession, Error> {
    let Some(measured_at) = s.medical_measured_at else {
        return Ok(s);
    };
    if s.alcohol_result.is_some()
        || s.tenko_type != "pre_operation"
        || s.status == "medical_pending"
    {
        return Ok(s);
    }
    let minutes = validity_minutes(tx, s.tenant_id, s.employee_id).await?;
    if !medical_expired(measured_at, minutes, now) {
        return Ok(s);
    }
    one(
        tx,
        sql::BACK_TO_MEDICAL,
        &[(&s.id, Type::UUID), (&s.tenant_id, Type::UUID)],
    )
    .await
}

// --- 口 ---

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/sessions", get(list_handler))
        .route("/tenko/sessions/start", post(start_handler))
        .route("/tenko/sessions/{id}", get(get_handler))
        .route("/tenko/sessions/{id}/medical", put(medical_handler))
        .route("/tenko/sessions/{id}/vehicle", put(vehicle_handler))
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
    headers: HeaderMap,
    Json(body): Json<StartSession>,
) -> Response {
    let device_id = device_id_from(&headers, &state.log);
    let dev_request = dev.get();
    let resp = run(
        state,
        tenant_id,
        dev_request,
        "session start",
        false,
        move |tx| {
            Box::pin(async move {
                // 予定あり (自動点呼) / なし (遠隔点呼)
                // 血圧の記録が足りず遠隔点呼へ切り替えた理由 (切り替えないときは None)
                let mut switch_reason: Option<String> = None;
                let (tenko_type, method, manager, vehicle) = match body.schedule_id {
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
                        // 業務前の自動点呼は血圧の記録が 10 日分そろっている乗務員だけ。足りなければ予定は消費して、
                        // 対面点呼 (通常点呼) へ切り替える (dev 端末は切り替えず、キオスクが警告を出す)
                        if row.get::<_, String>(1) == "pre_operation" && !dev_request {
                            let days: i64 = tx
                                .query_typed_one(
                                    sql::BP_HISTORY_DAYS,
                                    &[
                                        (&tenant_id, Type::UUID),
                                        (&body.employee_id, Type::UUID),
                                    ],
                                )
                                .await?
                                .get(0);
                            if days < BP_HISTORY_DAYS_REQUIRED {
                                switch_reason = Some(format!(
                                    "血圧の記録が {BP_HISTORY_DAYS_REQUIRED} 日分に満たない (いま {days} 日分) ため対面点呼"
                                ));
                            }
                        }
                        tx.execute_typed(
                            sql::CONSUME_SCHEDULE,
                            &[(&sid, Type::UUID), (&tenant_id, Type::UUID)],
                        )
                        .await?;
                        (
                            row.get::<_, String>(1),
                            AUTO,
                            Some(row.get::<_, String>(2)),
                            clean(row.get::<_, Option<String>>(3).as_deref()),
                        )
                    }
                    None => (
                        body.tenko_type
                            .clone()
                            .unwrap_or_else(|| "pre_operation".to_owned()),
                        REMOTE,
                        None,
                        None,
                    ),
                };
                // 予定に車両番号があればセッションへ写す (取得元は schedule)
                let vehicle_source = vehicle.as_ref().map(|_| "schedule");
                // 業務前は体温・血圧から始める
                let status = if tenko_type == "pre_operation" {
                    "medical_pending"
                } else {
                    "identity_verified"
                };
                // 点呼の値 (体温・血圧・アルコール) の置き場の測定を先に作り、セッションから指す。キオスクはこの id で
                // アルコールを保存し、端末の生の記録にも載せる
                let measurement_id: Uuid = tx
                    .query_typed_one(
                        sql::START_MEASUREMENT,
                        &[
                            (&tenant_id, Type::UUID),
                            (&body.employee_id, Type::UUID),
                            (&device_id, Type::TEXT),
                        ],
                    )
                    .await?
                    .get(0);
                let mut s = one(
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
                        (&measurement_id, Type::UUID),
                        (&vehicle, Type::TEXT),
                        (&vehicle_source, Type::TEXT),
                        (&device_id, Type::TEXT),
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
                // 対面点呼への切り替え: 方法を通常点呼にして理由を残す (状態は medical_pending のまま)
                if let Some(reason) = switch_reason {
                    s = one(
                        tx,
                        sql::SWITCH_IN_PERSON,
                        &[
                            (&reason, Type::TEXT),
                            (&s.id, Type::UUID),
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
    let at = now(&state);
    let dev_axis = dev.get();
    run(
        state,
        tenant_id,
        dev_axis,
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
                // 測定から有効時間 (社員の基準値。無ければ 30 分) を過ぎた値は受けない (業務前 八)
                let minutes = validity_minutes(tx, tenant_id, cur.employee_id).await?;
                if let Some(measured_at) = body.medical_measured_at {
                    if medical_expired(measured_at, minutes, at) {
                        return Ok(Flow::Api(bad_request(
                            "measurement_expired",
                            &format!(
                                "測定から有効時間 ({minutes} 分) を過ぎています。もう一度測定してください"
                            ),
                        )));
                    }
                }
                // 血圧は点呼のキオスクではなく測定台で測る (オーナー決定 2026-11-05)。本文に血圧が無ければ、有効時間内の
                // 最新の測定台の血圧を点呼の測定に写し、基準値との差と安全判定に使う (業務前 七・八・十)。測定の時刻は
                // 体温と血圧の早い方にする (再開のときの有効時間の見直しが古い方で効く)
                let mut bp = (body.systolic, body.diastolic, body.pulse);
                let mut measured_at = body.medical_measured_at;
                let mut stand_missing = false;
                if body.systolic.is_none() || body.diastolic.is_none() {
                    let stand = tx
                        .query_typed_opt(
                            sql::LATEST_BP,
                            &[(&tenant_id, Type::UUID), (&cur.employee_id, Type::UUID)],
                        )
                        .await?
                        .map(|r| stand_bp(&r))
                        .filter(|b| !medical_expired(b.measured_at, minutes, at));
                    match stand {
                        Some(b) => {
                            bp = (Some(b.systolic), Some(b.diastolic), body.pulse.or(b.pulse));
                            let temp_at = body.medical_measured_at.unwrap_or(at);
                            measured_at = Some(temp_at.min(b.measured_at));
                        }
                        None => stand_missing = true,
                    }
                }
                // 有効な測定台の血圧が無い自動点呼は、そのまま遠隔点呼として続ける (運行管理者が通話で確かめる。
                // オーナー決定 2026-11-05)。dev 端末は切り替えない (10 日分のルールと同じ。キオスクが警告する)
                let escalate = stand_missing && cur.tenko_method == AUTO && !dev_axis;
                // 値の置き場の測定に先に書く (セッションの応答はその測定から読む)。測定を持たない古いセッションは、
                // 開始の口と同じ started の測定を作って結ぶ
                let mid: Uuid = match cur.measurement_id {
                    Some(mid) => mid,
                    None => {
                        let device: Option<String> = None;
                        let mid: Uuid = tx
                            .query_typed_one(
                                sql::START_MEASUREMENT,
                                &[
                                    (&tenant_id, Type::UUID),
                                    (&cur.employee_id, Type::UUID),
                                    (&device, Type::TEXT),
                                ],
                            )
                            .await?
                            .get(0);
                        tx.execute_typed(
                            sql::SET_MEASUREMENT,
                            &[
                                (&mid, Type::UUID),
                                (&id, Type::UUID),
                                (&tenant_id, Type::UUID),
                            ],
                        )
                        .await?;
                        mid
                    }
                };
                tx.execute_typed(
                    sql::MEDICAL_TO_MEASUREMENT,
                    &[
                        (&body.temperature, Type::FLOAT8),
                        (&bp.0, Type::INT4),
                        (&bp.1, Type::INT4),
                        (&bp.2, Type::INT4),
                        (&measured_at, Type::TIMESTAMPTZ),
                        (&body.medical_manual_input, Type::BOOL),
                        (&mid, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                // セッションは段だけ進める (値は測定に書いた。応答の値はその測定から読む)
                let s = one(
                    tx,
                    sql::UPDATE_MEDICAL,
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
                if !escalate {
                    return Ok(Flow::Session(Box::new(s), None));
                }
                let reason = STAND_BP_MISSING_REASON.to_owned();
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
                // 業務前は車両番号が入るまで終えない (業務前 二十 ハ)。遠隔点呼は運行管理者が通話で確かめる
                // (キオスクも車両番号の段を挟まない)
                if cur.tenko_type == "pre_operation"
                    && cur.tenko_method != REMOTE
                    && clean(cur.vehicle_number.as_deref()).is_none()
                {
                    return Ok(Flow::Api(bad_request(
                        "vehicle_required",
                        "車両番号を登録してから点呼を終えてください",
                    )));
                }
                let next = finished_status(&cur.tenko_method, cur.schedule_id);
                let args: [Arg<'_>; 3] = [
                    (&next, Type::TEXT),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                // 判定待ちは記録を書かない (判定で書く)
                if next != "completed" {
                    let s = one(tx, sql::CONFIRM_INSTRUCTION, &args).await?;
                    return Ok(Flow::Session(Box::new(s), None));
                }
                Ok(
                    match close_with_record(tx, one(tx, sql::CONFIRM_INSTRUCTION, &args), &log)
                        .await?
                    {
                        Ok(s) => Flow::Session(Box::new(s), None),
                        Err(e) => Flow::Api(e),
                    },
                )
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
                    finished_status(&cur.tenko_method, cur.schedule_id)
                };
                let completed_at = (next == "completed").then_some(at);
                let args: [Arg<'_>; 8] = [
                    (&next, Type::TEXT),
                    (&body.vehicle_road_status, Type::TEXT),
                    (&body.driver_alternation, Type::TEXT),
                    (&body.vehicle_road_audio_url, Type::TEXT),
                    (&body.driver_alternation_audio_url, Type::TEXT),
                    (&completed_at, Type::TIMESTAMPTZ),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                let s = if next == "completed" {
                    match close_with_record(tx, one(tx, sql::UPDATE_REPORT, &args), &log).await? {
                        Ok(s) => s,
                        Err(e) => return Ok(Flow::Api(e)),
                    }
                } else {
                    one(tx, sql::UPDATE_REPORT, &args).await?
                };
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
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<Cancel>,
) -> Response {
    let log = state.log.clone();
    let prod_manager = prod_manager_key(&dev, user.is_some());
    let alternative_measure = clean(body.alternative_measure.as_deref());
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
                if beyond_prod_manager(prod_manager, &cur) {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
                // 判定待ちは乗務員の側が終わっている (閉じるのは運行管理者の判定)。ただし遠隔点呼の判定待ちは
                // 運行管理者を通話で呼んでいる間なので、出ないときに乗務員がやめられる (2026-11-01 ユーザー決定)。
                // IT点呼 の判定待ちは今までどおり取り消せない
                let closed = matches!(cur.status.as_str(), "completed" | "cancelled");
                let judging = cur.status == JUDGMENT_PENDING && cur.tenko_method != REMOTE;
                if closed || judging {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let args: [Arg<'_>; 4] = [
                    (&body.reason, Type::TEXT),
                    (&alternative_measure, Type::TEXT),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                Ok(
                    match close_with_record(tx, one(tx, sql::CANCEL, &args), &log).await? {
                        Ok(s) => Flow::Session(Box::new(s), None),
                        Err(e) => Flow::Api(e),
                    },
                )
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
                let args: [Arg<'_>; 5] = [
                    (&next, Type::TEXT),
                    (&judgment_json, Type::JSONB),
                    (&interrupted_at, Type::TIMESTAMPTZ),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                if pass {
                    let s = one(tx, sql::UPDATE_SAFETY_JUDGMENT, &args).await?;
                    return Ok(Flow::Session(Box::new(s), None));
                }
                let s =
                    match close_with_record(tx, one(tx, sql::UPDATE_SAFETY_JUDGMENT, &args), &log)
                        .await?
                    {
                        Ok(s) => s,
                        Err(e) => return Ok(Flow::Api(e)),
                    };
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
                let args: [Arg<'_>; 6] = [
                    (&next, Type::TEXT),
                    (&inspection, Type::JSONB),
                    (&cancel_reason, Type::TEXT),
                    (&completed_at, Type::TIMESTAMPTZ),
                    (&id, Type::UUID),
                    (&tenant_id, Type::UUID),
                ];
                if !has_ng {
                    let s = one(tx, sql::UPDATE_DAILY_INSPECTION, &args).await?;
                    // 携行品の確認が無く、アルコールの段の直前 (本人確認済み) へ進んだときは測定の有効時間を見る
                    let s = if next == "identity_verified" {
                        back_to_medical_if_expired(tx, s, at).await?
                    } else {
                        s
                    };
                    return Ok(Flow::Session(Box::new(s), None));
                }
                let s =
                    match close_with_record(tx, one(tx, sql::UPDATE_DAILY_INSPECTION, &args), &log)
                        .await?
                    {
                        Ok(s) => s,
                        Err(e) => return Ok(Flow::Api(e)),
                    };
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
                // アルコールの段の直前: 体温・血圧の測定が有効時間を過ぎていれば測り直しへ戻す
                let s = back_to_medical_if_expired(tx, s, at).await?;
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
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<Reason>,
) -> Response {
    let at = now(&state);
    let prod_manager = prod_manager_key(&dev, user.is_some());
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
                if beyond_prod_manager(prod_manager, &cur) {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
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
    user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    Json(body): Json<Resume>,
) -> Response {
    // 管理者ログイン (AuthUser が入る) か、運行管理者の鍵だけ通す。キオスクの鍵は弾く (キオスクは self-resume)
    if user.is_none() && !dev.is_tenko_manager() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body.reason.trim().is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let prod_manager = prod_manager_key(&dev, user.is_some());
    // 運行管理者の鍵からは NULL のまま (承認した管理者の列なので、ログインの無い席で埋めると嘘になる)
    let by = user.as_ref().map(|Extension(u)| u.user_id);
    // 再開を判断した運行管理者の氏名 (業務前 二十 ヨ)。ログインの利用者は省略時にその名前 (空ならメール)、運行管理者の鍵は必須
    let by_name = clean(body.resumed_by_name.as_deref()).or_else(|| {
        // kit の復元は name を空で入れるため (ヘッダーに名前が無い)、空ならメールで代える
        user.as_ref()
            .and_then(|Extension(u)| clean(Some(&u.name)).or_else(|| clean(Some(&u.email))))
    });
    let Some(by_name) = by_name else {
        return bad_request(
            "resumed_by_name_required",
            "再開した運行管理者の氏名は必須です",
        )
        .into_response();
    };
    let at = now(&state);
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
                if beyond_prod_manager(prod_manager, &cur) {
                    return Ok(StatusCode::FORBIDDEN.into());
                }
                if cur.status != "interrupted" {
                    return Ok(StatusCode::BAD_REQUEST.into());
                }
                let to = resume_to(&cur);
                let s = one(
                    tx,
                    sql::RESUME,
                    &[
                        (&to, Type::TEXT),
                        (&body.reason, Type::TEXT),
                        (&by, Type::UUID),
                        (&by_name, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                // 再開先がアルコールの段の前で、測定が有効時間を過ぎていれば体温・血圧の測り直しから
                let s = back_to_medical_if_expired(tx, s, at).await?;
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
    // 本人確認 (業務前 十三): 照合で決まった社員と方法が無ければ受けない (鍵だけでは再開させない)
    let Some(identity) = body
        .identity
        .filter(|i| RESUME_IDENTITY_METHODS.contains(&i.method.as_str()))
    else {
        return bad_request(
            "identity_required",
            "本人確認 (指静脈) をしてから再開してください",
        )
        .into_response();
    };
    // キオスク (端末の鍵 = AuthUser 無し) からは NULL のまま (承認した管理者の列なので埋めると嘘になる)
    let by = user.map(|Extension(u)| u.user_id);
    let at = now(&state);
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
                // 照合で決まった人が点呼の乗務員でなければ再開させない
                if identity.employee_id != cur.employee_id {
                    return Ok(Flow::Api((
                        StatusCode::FORBIDDEN,
                        Json(json!({
                            "error": "identity_mismatch",
                            "message": "本人確認の結果が点呼の乗務員と一致しません。本人の指を置いてください",
                        })),
                    )));
                }
                // 管理者が再開済みでも受ける (再開の時刻・理由・管理者は上書きしない)
                let s = one(
                    tx,
                    sql::SELF_RESUME,
                    &[
                        (&reason, Type::TEXT),
                        (&by, Type::UUID),
                        (&identity.method, Type::TEXT),
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                    ],
                )
                .await?;
                // 測定が有効時間を過ぎていれば体温・血圧の測り直しから
                let s = back_to_medical_if_expired(tx, s, at).await?;
                Ok(Flow::Session(Box::new(s), None))
            })
        },
    )
    .await
}

/// 車両番号 `PUT /tenko/sessions/{id}/vehicle` (自己点検表 業務前 二十 ハ)。終わっていないセッションだけ。
/// 車検証の管理番号が car_inspection に一致すればその登録番号 (取得元 `carins`)、
/// 一致しない・無ければ入力の車両番号 (予定の値と同じなら `schedule`、違えば `manual`)。どちらも無ければ 400
async fn vehicle_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<SetVehicle>,
) -> Response {
    let cert_no = clean(body.carins_cert_no.as_deref());
    let typed = clean(body.vehicle_number.as_deref());
    if cert_no.is_none() && typed.is_none() {
        return bad_request(
            "vehicle_required",
            "車検証の番号か車両番号を入力してください",
        )
        .into_response();
    }
    run(
        state,
        tenant_id,
        dev.get(),
        "session vehicle",
        true,
        move |tx| {
            Box::pin(async move {
                let Some(cur) = fetch(tx, tenant_id, id).await? else {
                    return Ok(Flow::Api(not_found("session_not_found")));
                };
                if is_terminal(&cur.status) {
                    return Ok(Flow::Api(bad_request(
                        "session_not_editable",
                        "この点呼は終了しているため車両番号を変更できません",
                    )));
                }
                let found = match cert_no.as_deref() {
                    Some(c) => lookup_expiry(tx, tenant_id, Some(c), None).await?,
                    None => CarinsLookup {
                        expires_on: None,
                        matched_by: "none",
                        car_no: None,
                    },
                };
                // 車検証に一致したときだけ carins の列も埋める (一致しなければ既存の値を保つ)
                let matched = found.car_no.is_some();
                let carins_cert: Option<&str> = cert_no.as_deref().filter(|_| matched);
                let carins_expires: Option<chrono::NaiveDate> =
                    found.expires_on.filter(|_| matched);
                let carins_matched: Option<&str> = matched.then_some(found.matched_by);
                let (vehicle, source) = match found.car_no {
                    Some(car_no) => (car_no, "carins"),
                    None => {
                        let Some(typed) = typed else {
                            return Ok(Flow::Api(bad_request(
                                "vehicle_required",
                                "車検証が見つかりません。車両番号を入力してください",
                            )));
                        };
                        let scheduled: Option<String> = match cur.schedule_id {
                            Some(sid) => tx
                                .query_typed_opt(
                                    sql::SCHEDULE_VEHICLE,
                                    &[(&sid, Type::UUID), (&tenant_id, Type::UUID)],
                                )
                                .await?
                                .and_then(|r| r.get(0)),
                            None => None,
                        };
                        let source = if scheduled.as_deref().map(str::trim) == Some(typed.as_str())
                        {
                            "schedule"
                        } else {
                            "manual"
                        };
                        (typed, source)
                    }
                };
                let s = one(
                    tx,
                    sql::SET_VEHICLE,
                    &[
                        (&vehicle, Type::TEXT),
                        (&source, Type::TEXT),
                        (&carins_cert, Type::TEXT),
                        (&carins_expires, Type::DATE),
                        (&carins_matched, Type::TEXT),
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
        let sid = Some(Uuid::nil());
        assert_eq!(finished_status("遠隔点呼", None), "judgment_pending");
        assert_eq!(finished_status("自動点呼", sid), "completed");
        // 予定から切り替わった対面点呼だけ判定待ち。測定から作る通常点呼 (予定なし) は完了
        assert_eq!(finished_status("通常点呼", sid), "judgment_pending");
        assert_eq!(finished_status("通常点呼", None), "completed");
    }

    #[test]
    fn medical_expiry_and_clean() {
        let now: DateTime<Utc> = "2026-10-22T22:00:00Z".parse().unwrap();
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        // ちょうど有効時間は有効、1 秒過ぎたら過ぎている
        assert!(!medical_expired(at("2026-10-22T21:30:00Z"), 30, now));
        assert!(medical_expired(at("2026-10-22T21:29:59Z"), 30, now));
        assert_eq!(clean(Some("  品川 ")), Some("品川".to_owned()));
        assert_eq!(clean(Some("  ")), None);
        assert_eq!(clean(None), None);
    }

    #[test]
    fn paging_defaults_and_caps() {
        assert_eq!(paging(None, None), (1, 50, 0));
        assert_eq!(paging(Some(0), Some(500)), (1, 100, 0));
        assert_eq!(paging(Some(3), Some(10)), (3, 10, 20));
    }
}
