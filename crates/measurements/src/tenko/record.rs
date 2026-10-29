//! 点呼の記録の規則の唯一の置き場 (Refs ippoan/rust-alc-api#680)。点呼セッションの行 ([`TenkoSession`])・列の順
//! ([`SESSION_COLUMNS`])・アルコールの結果の閉じ方 ([`status_for_result`])・記録の hash ([`record_payload`])・記録の INSERT
//! ([`write_record`])・内側の区切り ([`in_savepoint`])。
//!
//! 通常点呼・IT点呼 ([`super::normal`])、自動点呼・遠隔点呼の段 ([`super::flow`])、測定の保存で進めるアルコールの段
//! ([`super::alcohol`])、運行管理者の判定 ([`super::judgment`]) が全部ここを使う。rust-alc-api と ippoan/alc-tenko-worker に
//! 在った写しは、点呼の記録の入口をこの worker に一本化したので持たない (点呼の記録を書く worker はここだけ)。
//!
//! **record_data・record_hash の作り方を変えない** (既に書かれた記録の hash と比べられなくなる)。

use std::future::Future;

use alc_worker_db::{kind, TenantTx, TxOutput};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

/// 点呼セッションの行 (rust の `alc_tenko::models::TenkoSession`)。**欄の名前と型は rust と同じにする** —
/// 点呼記録の `record_data` はこの値の JSON、`record_hash` はその SHA-256。
/// 表の `is_dev` は rust の行型に無いので読まない (record_data にも入らない)。列の読み順は [`SESSION_COLUMNS`]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TenkoSession {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub employee_id: Uuid,
    pub schedule_id: Option<Uuid>,
    pub tenko_type: String,
    pub tenko_method: String,
    pub status: String,
    pub identity_verified_at: Option<DateTime<Utc>>,
    pub identity_face_photo_url: Option<String>,
    pub measurement_id: Option<Uuid>,
    pub alcohol_result: Option<String>,
    pub alcohol_value: Option<f64>,
    pub alcohol_tested_at: Option<DateTime<Utc>>,
    pub alcohol_face_photo_url: Option<String>,
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub medical_manual_input: Option<bool>,
    pub instruction_confirmed_at: Option<DateTime<Utc>>,
    pub report_vehicle_road_status: Option<String>,
    pub report_driver_alternation: Option<String>,
    pub report_no_report: Option<bool>,
    pub report_vehicle_road_audio_url: Option<String>,
    pub report_driver_alternation_audio_url: Option<String>,
    pub report_submitted_at: Option<DateTime<Utc>>,
    pub location: Option<String>,
    pub responsible_manager_name: Option<String>,
    pub cancel_reason: Option<String>,
    pub interrupted_at: Option<DateTime<Utc>>,
    pub resumed_at: Option<DateTime<Utc>>,
    pub resume_reason: Option<String>,
    pub resumed_by_user_id: Option<Uuid>,
    pub self_declaration: Option<serde_json::Value>,
    pub safety_judgment: Option<serde_json::Value>,
    pub daily_inspection: Option<serde_json::Value>,
    pub carrying_items_checked: Option<serde_json::Value>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub carins_cert_no: Option<String>,
    pub carins_vehicle_id: Option<String>,
    pub carins_expires_on: Option<NaiveDate>,
    pub carins_matched_by: Option<String>,
    pub escalated_to_remote_at: Option<DateTime<Utc>>,
    pub remote_escalation_reason: Option<String>,
    pub manager_judgment: Option<String>,
    pub manager_judgment_reason: Option<String>,
    pub manager_judgment_by: Option<Uuid>,
    pub manager_judgment_method: Option<String>,
    pub identity_method: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for TenkoSession {}

/// rust の `RETURNING *` / `SELECT *` の代わり。[`TenkoSession`] の欄の順 (Worker は位置で読む)
pub const SESSION_COLUMNS: &str = "id, tenant_id, employee_id, schedule_id, tenko_type, tenko_method, status, \
     identity_verified_at, identity_face_photo_url, measurement_id, alcohol_result, alcohol_value, \
     alcohol_tested_at, alcohol_face_photo_url, temperature, systolic, diastolic, pulse, \
     medical_measured_at, medical_manual_input, instruction_confirmed_at, report_vehicle_road_status, \
     report_driver_alternation, report_no_report, report_vehicle_road_audio_url, \
     report_driver_alternation_audio_url, report_submitted_at, location, responsible_manager_name, \
     cancel_reason, interrupted_at, resumed_at, resume_reason, resumed_by_user_id, self_declaration, \
     safety_judgment, daily_inspection, carrying_items_checked, started_at, completed_at, carins_cert_no, \
     carins_vehicle_id, carins_expires_on, carins_matched_by, escalated_to_remote_at, \
     remote_escalation_reason, manager_judgment, manager_judgment_reason, manager_judgment_by, \
     manager_judgment_method, identity_method, created_at, updated_at";

impl TenkoSession {
    pub fn from_row(r: &Row) -> Self {
        Self {
            id: r.get(0),
            tenant_id: r.get(1),
            employee_id: r.get(2),
            schedule_id: r.get(3),
            tenko_type: r.get(4),
            tenko_method: r.get(5),
            status: r.get(6),
            identity_verified_at: r.get(7),
            identity_face_photo_url: r.get(8),
            measurement_id: r.get(9),
            alcohol_result: r.get(10),
            alcohol_value: r.get(11),
            alcohol_tested_at: r.get(12),
            alcohol_face_photo_url: r.get(13),
            temperature: r.get(14),
            systolic: r.get(15),
            diastolic: r.get(16),
            pulse: r.get(17),
            medical_measured_at: r.get(18),
            medical_manual_input: r.get(19),
            instruction_confirmed_at: r.get(20),
            report_vehicle_road_status: r.get(21),
            report_driver_alternation: r.get(22),
            report_no_report: r.get(23),
            report_vehicle_road_audio_url: r.get(24),
            report_driver_alternation_audio_url: r.get(25),
            report_submitted_at: r.get(26),
            location: r.get(27),
            responsible_manager_name: r.get(28),
            cancel_reason: r.get(29),
            interrupted_at: r.get(30),
            resumed_at: r.get(31),
            resume_reason: r.get(32),
            resumed_by_user_id: r.get(33),
            self_declaration: r.get(34),
            safety_judgment: r.get(35),
            daily_inspection: r.get(36),
            carrying_items_checked: r.get(37),
            started_at: r.get(38),
            completed_at: r.get(39),
            carins_cert_no: r.get(40),
            carins_vehicle_id: r.get(41),
            carins_expires_on: r.get(42),
            carins_matched_by: r.get(43),
            escalated_to_remote_at: r.get(44),
            remote_escalation_reason: r.get(45),
            manager_judgment: r.get(46),
            manager_judgment_reason: r.get(47),
            manager_judgment_by: r.get(48),
            manager_judgment_method: r.get(49),
            identity_method: r.get(50),
            created_at: r.get(51),
            updated_at: r.get(52),
        }
    }
}

pub mod sql {
    /// 乗務員の名前 (記録・webhook。同じ transaction の中で引く)。$1 employee_id / $2 tenant_id。rust と同じく退職は見ない
    pub const EMPLOYEE_NAME: &str = "SELECT name FROM employees WHERE id = $1 AND tenant_id = $2";

    /// 予定の指示事項 (記録の `instruction`)。$1 schedule_id / $2 tenant_id
    pub const INSTRUCTION: &str =
        "SELECT instruction FROM tenko_schedules WHERE id = $1 AND tenant_id = $2";

    /// 点呼記録 (`tenko_records`) の INSERT (rust の repo/tenko_sessions.rs の `insert_record` と同じ 33 の値。
    /// rust の doc の「32 列」は tenko_method を足す前の数)
    pub const INSERT_RECORD: &str = "INSERT INTO tenko_records ( \
             tenant_id, session_id, employee_id, tenko_type, status, \
             record_data, employee_name, responsible_manager_name, \
             location, alcohol_result, alcohol_value, alcohol_has_face_photo, \
             temperature, systolic, diastolic, pulse, \
             instruction, instruction_confirmed_at, \
             report_vehicle_road_status, report_driver_alternation, report_no_report, \
             report_vehicle_road_audio_url, report_driver_alternation_audio_url, \
             started_at, completed_at, record_hash, \
             self_declaration, safety_judgment, daily_inspection, \
             interrupted_at, resumed_at, resume_reason, \
             tenko_method \
         ) VALUES ( \
             $1, $2, $3, $4, $5, \
             $6, $7, $8, \
             $9, $10, $11, $12, \
             $13, $14, $15, $16, \
             $17, $18, \
             $19, $20, $21, \
             $22, $23, \
             $24, $25, $26, \
             $27, $28, $29, \
             $30, $31, $32, \
             $33 \
         )";
}

/// 内側の区切りの名前 (固定。入力から作らない)。`begin`・`release`・`rollback` は型付きの 1 文として流す
/// (kit に入れ子の transaction の口が無いため。名前付き prepared statement にはならない)
pub struct Savepoint {
    pub begin: &'static str,
    pub release: &'static str,
    pub rollback: &'static str,
}

/// 段が閉じたときの点呼の記録の区切り (失敗しても段の更新は残す)
pub const SP_RECORD: Savepoint = Savepoint {
    begin: "SAVEPOINT tenko_record",
    release: "RELEASE SAVEPOINT tenko_record",
    rollback: "ROLLBACK TO SAVEPOINT tenko_record",
};

/// `body` を区切り `sp` の中で流す (rust の sqlx の入れ子 transaction と同じ働き)。
/// 成功なら区切りを解き、失敗なら区切りまで戻して外側の transaction を使える状態に戻す。
/// `Err` はログに出す固定の語 (段と SQLSTATE だけ)
pub async fn in_savepoint<T>(
    tx: &TenantTx<'_>,
    sp: &Savepoint,
    body: impl Future<Output = Result<T, Error>>,
) -> Result<T, String> {
    let fail = |stage: &str, e: &Error| format!("{stage} ({})", kind(e));
    tx.execute_typed(sp.begin, &[])
        .await
        .map_err(|e| fail("begin", &e))?;
    match body.await {
        Ok(v) => tx
            .execute_typed(sp.release, &[])
            .await
            .map(|_| v)
            .map_err(|e| fail("release", &e)),
        Err(e) => {
            let rollback = tx.execute_typed(sp.rollback, &[]).await;
            Err(fail("body", &e) + if rollback.is_ok() { "" } else { " +rollback" })
        }
    }
}

/// アルコール測定の結果から「点呼をどう閉じるか」を決める (rust の `status_for_result`。全部の点呼で 1 本)。
///
/// * `fail` / `over` → 中止 (`cancelled`) + 中止理由「アルコール検知」
/// * `pass` / `normal` → 完了 (`completed`)
/// * `error` / 未知の値 / 無し → `None` (点呼として閉じない = 記録を作らない)
pub fn status_for_result(result: Option<&str>) -> Option<(&'static str, Option<&'static str>)> {
    match result {
        Some("fail") | Some("over") => Some(("cancelled", Some("アルコール検知"))),
        Some("pass") | Some("normal") => Some(("completed", None)),
        _ => None,
    }
}

/// 点呼記録の `record_data` と `record_hash` を作る (rust の `record_payload`)。
/// hash はセッションの JSON をそのまま SHA-256 にかけたもの (作り方は変えない。serde_json の Map は key の昇順)
pub fn record_payload(session: &TenkoSession) -> (serde_json::Value, String) {
    // 欄は全部 JSON にできる型 (文字列の key の構造体) なので失敗しない
    let record_data = serde_json::to_value(session).expect("TenkoSession は JSON にできる");
    let canonical = record_data.to_string();
    let hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    (record_data, hash)
}

/// 乗務員の名前 (無ければ `None`)
pub async fn employee_name(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<Option<String>, Error> {
    Ok(tx
        .query_typed_opt(
            sql::EMPLOYEE_NAME,
            &[(&employee_id, Type::UUID), (&tenant_id, Type::UUID)],
        )
        .await?
        .map(|r| r.get(0)))
}

/// 閉じたセッションの点呼記録を 1 件書く (rust の `create_tenko_record` / `insert_record`)。
/// 社員の名前は同じ transaction で引き (無ければ空)、指示事項は予定から引く (予定が無ければ NULL)。
/// 点呼方法はセッションの列のまま (遠隔点呼へ切り替えたセッションを自動点呼として記録しない)
pub async fn write_record(tx: &TenantTx<'_>, s: &TenkoSession) -> Result<(), Error> {
    let name = employee_name(tx, s.tenant_id, s.employee_id)
        .await?
        .unwrap_or_default();
    let instruction: Option<String> = match s.schedule_id {
        Some(sid) => tx
            .query_typed_opt(
                sql::INSTRUCTION,
                &[(&sid, Type::UUID), (&s.tenant_id, Type::UUID)],
            )
            .await?
            .and_then(|r| r.get(0)),
        None => None,
    };
    let (record_data, record_hash) = record_payload(s);
    let has_face_photo = s.alcohol_face_photo_url.is_some();
    tx.execute_typed(
        sql::INSERT_RECORD,
        &[
            (&s.tenant_id, Type::UUID),
            (&s.id, Type::UUID),
            (&s.employee_id, Type::UUID),
            (&s.tenko_type, Type::TEXT),
            (&s.status, Type::TEXT),
            (&record_data, Type::JSONB),
            (&name, Type::TEXT),
            (&s.responsible_manager_name, Type::TEXT),
            (&s.location, Type::TEXT),
            (&s.alcohol_result, Type::TEXT),
            (&s.alcohol_value, Type::FLOAT8),
            (&has_face_photo, Type::BOOL),
            (&s.temperature, Type::FLOAT8),
            (&s.systolic, Type::INT4),
            (&s.diastolic, Type::INT4),
            (&s.pulse, Type::INT4),
            (&instruction, Type::TEXT),
            (&s.instruction_confirmed_at, Type::TIMESTAMPTZ),
            (&s.report_vehicle_road_status, Type::TEXT),
            (&s.report_driver_alternation, Type::TEXT),
            (&s.report_no_report, Type::BOOL),
            (&s.report_vehicle_road_audio_url, Type::TEXT),
            (&s.report_driver_alternation_audio_url, Type::TEXT),
            (&s.started_at, Type::TIMESTAMPTZ),
            (&s.completed_at, Type::TIMESTAMPTZ),
            (&record_hash, Type::TEXT),
            (&s.self_declaration, Type::JSONB),
            (&s.safety_judgment, Type::JSONB),
            (&s.daily_inspection, Type::JSONB),
            (&s.interrupted_at, Type::TIMESTAMPTZ),
            (&s.resumed_at, Type::TIMESTAMPTZ),
            (&s.resume_reason, Type::TEXT),
            (&s.tenko_method, Type::TEXT),
        ],
    )
    .await?;
    Ok(())
}

/// 段が閉じたときの記録 (失敗しても止めない。rust の `let _ = create_tenko_record(..)`)。区切り [`SP_RECORD`] の中で書き、
/// 失敗は固定の語でログに出す
pub async fn record_or_log(tx: &TenantTx<'_>, s: &TenkoSession, log: &crate::LogSink) {
    if let Err(stage) = in_savepoint(tx, &SP_RECORD, write_record(tx, s)).await {
        log(&format!("measurements: tenko record failed: {stage}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_for_result_follows_rust_rules() {
        assert_eq!(
            status_for_result(Some("fail")),
            Some(("cancelled", Some("アルコール検知")))
        );
        assert_eq!(
            status_for_result(Some("over")),
            Some(("cancelled", Some("アルコール検知")))
        );
        assert_eq!(status_for_result(Some("pass")), Some(("completed", None)));
        assert_eq!(status_for_result(Some("normal")), Some(("completed", None)));
        assert_eq!(status_for_result(Some("error")), None);
        assert_eq!(status_for_result(Some("unknown")), None);
        assert_eq!(status_for_result(None), None);
    }

    #[test]
    fn session_columns_match_the_struct() {
        // 位置で読むので、列の数と欄の数が合っていること (from_row の最後の位置 52 + 1)
        assert_eq!(SESSION_COLUMNS.split(',').count(), 53);
        let json = serde_json::to_value(sample_session()).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 53);
    }

    pub(crate) fn sample_session() -> TenkoSession {
        let at = DateTime::parse_from_rfc3339("2026-09-12T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        TenkoSession {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            employee_id: Uuid::nil(),
            schedule_id: None,
            tenko_type: "normal".into(),
            tenko_method: "通常点呼".into(),
            status: "completed".into(),
            identity_verified_at: None,
            identity_face_photo_url: None,
            measurement_id: Some(Uuid::nil()),
            alcohol_result: Some("normal".into()),
            alcohol_value: Some(0.0),
            alcohol_tested_at: Some(at),
            alcohol_face_photo_url: None,
            temperature: Some(36.5),
            systolic: Some(120),
            diastolic: Some(80),
            pulse: Some(64),
            medical_measured_at: None,
            medical_manual_input: None,
            instruction_confirmed_at: None,
            report_vehicle_road_status: None,
            report_driver_alternation: None,
            report_no_report: None,
            report_vehicle_road_audio_url: None,
            report_driver_alternation_audio_url: None,
            report_submitted_at: None,
            location: None,
            responsible_manager_name: None,
            cancel_reason: None,
            interrupted_at: None,
            resumed_at: None,
            resume_reason: None,
            resumed_by_user_id: None,
            self_declaration: None,
            safety_judgment: None,
            daily_inspection: None,
            carrying_items_checked: None,
            started_at: Some(at),
            completed_at: Some(at),
            carins_cert_no: None,
            carins_vehicle_id: None,
            carins_expires_on: NaiveDate::from_ymd_opt(2030, 12, 31),
            carins_matched_by: Some("cert_no".into()),
            escalated_to_remote_at: None,
            remote_escalation_reason: None,
            manager_judgment: None,
            manager_judgment_reason: None,
            manager_judgment_by: None,
            manager_judgment_method: None,
            identity_method: None,
            created_at: at,
            updated_at: at,
        }
    }

    #[test]
    fn record_payload_hashes_the_sorted_json() {
        let (data, hash) = record_payload(&sample_session());
        let text = data.to_string();
        // key の昇順 (serde_json の Map = BTreeMap。preserve_order を付けていない)
        assert!(text.starts_with(r#"{"alcohol_face_photo_url":null,"alcohol_result":"normal","#));
        assert!(text.contains(r#""carins_expires_on":"2030-12-31""#));
        assert!(text.contains(r#""started_at":"2026-09-12T01:00:00Z""#));
        assert_eq!(hash, format!("{:x}", Sha256::digest(text.as_bytes())));
        assert_eq!(hash.len(), 64);
    }
}
