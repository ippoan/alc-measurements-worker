//! 通常点呼 (運行者端末の測定) を点呼セッション・点呼記録に載せる (Refs ippoan/alc-app#238, ippoan/alc-app-s3#135)。
//! rust-alc-api の次の写し:
//!
//! - `alc-misc` の repo/measurements.rs の `record_as_tenko_if_marked`・`record_or_existing_session_id`
//! - `alc-tenko` の normal_tenko.rs (`record`・`existing_session_id`・`carins_expiry`)
//! - `alc-tenko` の tenko_sessions.rs の `status_for_result`・`record_payload` と、repo/tenko_sessions.rs の `insert_record`
//!
//! 運行者端末は管理者ログイン無しで動くため、点呼の段 (`submit_medical` → `submit_alcohol` → …) を踏まない。
//! 代わりに **測定の保存と同じ transaction** の中でセッションと記録を 1 組作る。記録の作成は内側の区切り
//! (SAVEPOINT) で行い、失敗したら内側だけ戻す — **測定の保存は必ず残す**。
//!
//! **rust と 2 か所に在る** (rust の自動点呼・遠隔点呼が同じ `status_for_result`・`record_payload`・`insert_record` を
//! 使うので、rust からは消せない)。**片方を変えたらもう片方も変える** (列・規則・hash の作り方)。

use std::future::Future;

use alc_worker_db::{kind, TenantTx};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::carins::lookup_expiry;
use crate::models::Measurement;
use crate::LogSink;

/// 通常点呼の種別の既定値。運行者端末で始業 / 終業を選ぶと `pre_operation` / `post_operation` が渡ってくる
pub const TENKO_TYPE_NORMAL: &str = "normal";

/// 点呼セッション・点呼記録の点呼方法の既定値。運行管理者の一覧・CSV にこの文字列が出る。
/// 測定の保存 (PUT) が `IT点呼` を指定してきたときだけ、そちらで記録する (Refs ippoan/alc-app#387)
pub const TENKO_METHOD_NORMAL: &str = "通常点呼";

/// 測定の保存に付いてくる、通常点呼の記録だけに使う値 (値の検査は handler で済ませてある)。
/// rust の `NormalTenkoInput` (tenant_tx の閉包に持ち込むので owned)。
///
/// 確定を後回しにする保存 (`defer_tenko`) は、点呼方法を除いたこの値を JSON で測定の列 `tenko_pending` に入れ、
/// 確定のとき ([`crate::deferred`]) に読み戻して点呼方法を足す (Refs ippoan/alc-app#436)
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NormalTenkoInput {
    /// `None` → `normal`
    pub tenko_type: Option<String>,
    /// `None` → `通常点呼`。session と record の両方に同じ値を書く。`tenko_pending` には入れない (確定のときに決まる)
    #[serde(skip)]
    pub tenko_method: Option<String>,
    /// 運転者の本人確認の方法 (migration 159)。`None` → NULL (記録なし)。
    /// session の列にだけ書く (record には `record_data` 経由で入る)
    pub identity_method: Option<String>,
    /// 運行者端末が電子車検証から読んだ管理番号 (Refs ippoan/alc-app-s3#110)
    pub carins_cert_no: Option<String>,
    /// 同じく車両 ID
    pub carins_vehicle_id: Option<String>,
}

/// 点呼セッションの行 (rust の `alc_tenko::models::TenkoSession`)。**欄の名前と型は rust と同じにする** —
/// 点呼記録の `record_data` はこの値の JSON、`record_hash` はその SHA-256 で、自動点呼の記録と同じ作り方。
/// 表の `is_dev` は rust の行型に無いので読まない (record_data にも入らない)。列の読み順は [`SESSION_COLUMNS`]
#[derive(Debug, Clone, Serialize)]
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

/// rust の `RETURNING *` の代わり。[`TenkoSession`] の欄の順 (Worker は位置で読む)
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
    fn from_row(r: &Row) -> Self {
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
    /// 通常点呼のセッション (rust の normal_tenko.rs の `record` の INSERT と同じ列・同じ値)。
    /// 記録済み (migration 141 の部分 unique。同じ測定、または同じ乗務員・同じ測定時刻) は `ON CONFLICT DO NOTHING`
    /// で 0 行 (先勝ち)
    pub fn insert_session() -> String {
        format!(
            "INSERT INTO tenko_sessions ( \
                 tenant_id, employee_id, schedule_id, tenko_type, status, \
                 measurement_id, alcohol_result, alcohol_value, alcohol_tested_at, \
                 alcohol_face_photo_url, \
                 temperature, systolic, diastolic, pulse, medical_measured_at, \
                 medical_manual_input, \
                 responsible_manager_name, cancel_reason, \
                 started_at, completed_at, tenko_method, \
                 carins_cert_no, carins_vehicle_id, carins_expires_on, carins_matched_by, \
                 identity_method \
             ) VALUES ( \
                 $1, $2, NULL, $3, $4, \
                 $5, $6, $7, $8, \
                 $9, \
                 $10, $11, $12, $13, $14, \
                 $15, \
                 NULL, $16, \
                 $17, NOW(), $18, \
                 $19, $20, $21, $22, \
                 $23 \
             ) \
             ON CONFLICT DO NOTHING \
             RETURNING {}",
            super::SESSION_COLUMNS
        )
    }

    /// 乗務員の名前 (同じ transaction の中で引く)
    pub const EMPLOYEE_NAME: &str = "SELECT name FROM employees WHERE id = $1 AND tenant_id = $2";

    /// 同じ測定に既にある、通常の流れ (通常点呼 / IT点呼) のセッションの id と点呼方法。述語は冪等用の部分 unique と同じで、
    /// 自動点呼・遠隔点呼が同じ測定を指している session は拾わない
    pub const EXISTING_SESSION: &str = "SELECT id, tenko_method FROM tenko_sessions \
         WHERE measurement_id = $1 AND tenant_id = $2 \
           AND (tenko_type = 'normal' OR tenko_method IN ('通常点呼', 'IT点呼')) \
         LIMIT 1";

    /// 点呼記録 (`tenko_records`) の INSERT (rust の repo/tenko_sessions.rs の `insert_record` と同じ 33 の値。
    /// rust の doc の「32 列」は tenko_method を足す前の数)。rust は `RETURNING *` で記録の行を返すが、
    /// 通常点呼の呼び元は使わないので返さない
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

/// 点呼の記録 (セッション + 記録) の区切り
pub const SP_TENKO: Savepoint = Savepoint {
    begin: "SAVEPOINT normal_tenko_record",
    release: "RELEASE SAVEPOINT normal_tenko_record",
    rollback: "ROLLBACK TO SAVEPOINT normal_tenko_record",
};

/// 車検証の照合の区切り (点呼の記録の区切りの内側)
pub const SP_CARINS: Savepoint = Savepoint {
    begin: "SAVEPOINT carins_lookup",
    release: "RELEASE SAVEPOINT carins_lookup",
    rollback: "ROLLBACK TO SAVEPOINT carins_lookup",
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

/// アルコール測定の結果から「点呼をどう閉じるか」を決める (rust の `status_for_result`。自動点呼と規則を 1 本に保つ)。
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

/// 点呼記録の INSERT 本体 (rust の `insert_record`。自動点呼 / 遠隔点呼の `create_tenko_record` と同じ列)。
/// **rust の自動点呼と同じ列。片方を変えたらもう片方も変える。**
#[allow(clippy::too_many_arguments)]
pub async fn insert_record(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    session: &TenkoSession,
    employee_name: &str,
    instruction: &Option<String>,
    record_data: &serde_json::Value,
    record_hash: &str,
    tenko_method: &str,
) -> Result<(), Error> {
    let has_face_photo = session.alcohol_face_photo_url.is_some();
    tx.execute_typed(
        sql::INSERT_RECORD,
        &[
            (&tenant_id, Type::UUID),
            (&session.id, Type::UUID),
            (&session.employee_id, Type::UUID),
            (&session.tenko_type, Type::TEXT),
            (&session.status, Type::TEXT),
            (record_data, Type::JSONB),
            (&employee_name, Type::TEXT),
            (&session.responsible_manager_name, Type::TEXT),
            (&session.location, Type::TEXT),
            (&session.alcohol_result, Type::TEXT),
            (&session.alcohol_value, Type::FLOAT8),
            (&has_face_photo, Type::BOOL),
            (&session.temperature, Type::FLOAT8),
            (&session.systolic, Type::INT4),
            (&session.diastolic, Type::INT4),
            (&session.pulse, Type::INT4),
            (instruction, Type::TEXT),
            (&session.instruction_confirmed_at, Type::TIMESTAMPTZ),
            (&session.report_vehicle_road_status, Type::TEXT),
            (&session.report_driver_alternation, Type::TEXT),
            (&session.report_no_report, Type::BOOL),
            (&session.report_vehicle_road_audio_url, Type::TEXT),
            (&session.report_driver_alternation_audio_url, Type::TEXT),
            (&session.started_at, Type::TIMESTAMPTZ),
            (&session.completed_at, Type::TIMESTAMPTZ),
            (&record_hash, Type::TEXT),
            (&session.self_declaration, Type::JSONB),
            (&session.safety_judgment, Type::JSONB),
            (&session.daily_inspection, Type::JSONB),
            (&session.interrupted_at, Type::TIMESTAMPTZ),
            (&session.resumed_at, Type::TIMESTAMPTZ),
            (&session.resume_reason, Type::TEXT),
            (&tenko_method, Type::TEXT),
        ],
    )
    .await?;
    Ok(())
}

/// 電子車検証の番号で carins を照合し、(期限, matched_by) を返す (rust の `carins_expiry`)。
///
/// * 番号が両方無い → `(None, None)` (照合しない)
/// * 照合は内側の区切り ([`SP_CARINS`]) で行う。SQL が失敗しても区切りだけ戻し、`(None, None)` で記録を続ける
///   — 失敗した文で外側の transaction を壊さないため。ログには番号を出さず段だけ残す
async fn carins_expiry(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    cert_no: Option<&str>,
    vehicle_id: Option<&str>,
    log: &LogSink,
) -> (Option<NaiveDate>, Option<&'static str>) {
    if cert_no.is_none() && vehicle_id.is_none() {
        return (None, None);
    }
    match in_savepoint(
        tx,
        &SP_CARINS,
        lookup_expiry(tx, tenant_id, cert_no, vehicle_id),
    )
    .await
    {
        Ok(found) => (found.expires_on, Some(found.matched_by)),
        Err(stage) => {
            log(&format!("measurements: carins lookup failed: {stage}"));
            (None, None)
        }
    }
}

/// 完了した通常点呼の測定から、点呼セッションと点呼記録を 1 組作る (rust の `normal_tenko::record`)。
///
/// * 結果が `error` / 無し → `Ok(None)` (測定だけ残す。エラーにしない)
/// * `tenko_type` が `None` → `normal`
/// * 既に記録済み (同じ測定、または同じ乗務員・同じ測定時刻) → `Ok(None)`
///   — migration 141 の部分 unique に `ON CONFLICT DO NOTHING` で任せる (先勝ち。2 回目の `identity_method` も書かない)
/// * 電子車検証の番号があれば、同じ transaction で carins を照合して期限を写す ([`carins_expiry`])。照合に失敗しても記録は作る
pub async fn record(
    tx: &TenantTx<'_>,
    m: &Measurement,
    input: &NormalTenkoInput,
    log: &LogSink,
) -> Result<Option<TenkoSession>, Error> {
    let Some((status, cancel_reason)) = status_for_result(m.result.as_deref()) else {
        return Ok(None);
    };
    let tenko_type = input.tenko_type.as_deref().unwrap_or(TENKO_TYPE_NORMAL);
    let tenko_method = input.tenko_method.as_deref().unwrap_or(TENKO_METHOD_NORMAL);
    let cert_no = input.carins_cert_no.as_deref();
    let vehicle_id = input.carins_vehicle_id.as_deref();
    let (carins_expires_on, carins_matched_by) =
        carins_expiry(tx, m.tenant_id, cert_no, vehicle_id, log).await;

    let session = tx
        .query_typed_opt(
            &sql::insert_session(),
            &[
                (&m.tenant_id, Type::UUID),
                (&m.employee_id, Type::UUID),
                (&tenko_type, Type::TEXT),
                (&status, Type::TEXT),
                (&m.id, Type::UUID),
                (&m.result, Type::TEXT),
                (&m.alcohol_level, Type::FLOAT8),
                (&m.measured_at, Type::TIMESTAMPTZ),
                (&m.face_photo_url, Type::TEXT),
                (&m.temperature, Type::FLOAT8),
                (&m.systolic, Type::INT4),
                (&m.diastolic, Type::INT4),
                (&m.pulse, Type::INT4),
                (&m.medical_measured_at, Type::TIMESTAMPTZ),
                (&m.medical_manual_input, Type::BOOL),
                (&cancel_reason, Type::TEXT),
                (&m.measured_at, Type::TIMESTAMPTZ),
                (&tenko_method, Type::TEXT),
                (&cert_no, Type::TEXT),
                (&vehicle_id, Type::TEXT),
                (&carins_expires_on, Type::DATE),
                (&carins_matched_by, Type::TEXT),
                (&input.identity_method, Type::TEXT),
            ],
        )
        .await?;
    // 既に記録済み — 部分 unique が弾いた (同じ測定への再送、またはオフライン保存で測定がもう 1 件できた経路)。
    // 二重に記録しない。
    let Some(session) = session.as_ref().map(TenkoSession::from_row) else {
        return Ok(None);
    };

    let employee_name: Option<String> = tx
        .query_typed_opt(
            sql::EMPLOYEE_NAME,
            &[(&m.employee_id, Type::UUID), (&m.tenant_id, Type::UUID)],
        )
        .await?
        .map(|r| r.get(0));
    let (record_data, record_hash) = record_payload(&session);
    insert_record(
        tx,
        m.tenant_id,
        &session,
        &employee_name.unwrap_or_default(),
        &None,
        &record_data,
        &record_hash,
        tenko_method,
    )
    .await?;
    Ok(Some(session))
}

/// 同じ測定に既にある、通常の流れ (通常点呼 / IT点呼) の点呼セッションの (id, 点呼方法) を引く。
/// 確定の口 ([`crate::deferred`]) は、既にある方法と頼まれた方法を比べるのに使う
pub async fn existing_session(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    measurement_id: Uuid,
) -> Result<Option<(Uuid, String)>, Error> {
    Ok(tx
        .query_typed_opt(
            sql::EXISTING_SESSION,
            &[(&measurement_id, Type::UUID), (&tenant_id, Type::UUID)],
        )
        .await?
        .map(|r| (r.get(0), r.get(1))))
}

/// 同じ測定に既にある、通常の流れ (通常点呼 / IT点呼) の点呼セッションの id を引く (rust の `existing_session_id`)。
/// [`record`] が `Ok(None)` (記録済み) を返したときに、測定の保存の応答へ載せる id を引き直すために使う
pub async fn existing_session_id(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    measurement_id: Uuid,
) -> Result<Option<Uuid>, Error> {
    Ok(existing_session(tx, tenant_id, measurement_id)
        .await?
        .map(|(id, _)| id))
}

/// 点呼セッションを作ってその id を返す。作られなかったとき (記録済みの再送、または結果が点呼にならない測定) は、
/// 同じ測定に既にある session の id を引き直す (rust の `record_or_existing_session_id`)
pub(crate) async fn record_or_existing_session_id(
    tx: &TenantTx<'_>,
    m: &Measurement,
    input: &NormalTenkoInput,
    log: &LogSink,
) -> Result<Option<Uuid>, Error> {
    match record(tx, m, input, log).await? {
        Some(session) => Ok(Some(session.id)),
        None => existing_session_id(tx, m.tenant_id, m.id).await,
    }
}

/// 通常点呼の印が付いた「完了した測定」から、点呼セッションと点呼記録を作る (rust の `record_as_tenko_if_marked`)。
///
/// **測定の保存は必ず残す** のがこの関数の契約。記録の作成は内側の区切り ([`SP_TENKO`]) で行い、失敗したら
/// 内側だけ戻してログを残す — 外側の測定の保存はそのまま commit される (Refs ippoan/alc-app#238, ippoan/alc-app-s3#135)。
///
/// 戻り値は点呼セッションの id (Refs ippoan/alc-app#387): 今回作ったもの、または同じ測定に既にあるもの (再送)。
/// 印が無い・結果が点呼にならない・記録に失敗したときは `None`。
pub async fn record_as_tenko_if_marked(
    tx: &TenantTx<'_>,
    m: &Measurement,
    record_as_tenko: bool,
    input: &NormalTenkoInput,
    log: &LogSink,
) -> Option<Uuid> {
    if !record_as_tenko || m.status != "completed" {
        return None;
    }
    let body = record_or_existing_session_id(tx, m, input, log);
    in_savepoint(tx, &SP_TENKO, body)
        .await
        .unwrap_or_else(|stage| {
            log(&format!(
                "measurements: normal tenko record failed: {stage}"
            ));
            None
        })
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

    fn sample_session() -> TenkoSession {
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
