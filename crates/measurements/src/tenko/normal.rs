//! 通常点呼・IT点呼 (運行者端末の測定) を点呼セッション・点呼記録に載せる (Refs ippoan/alc-app#238, ippoan/alc-app-s3#135)。
//! rust-alc-api の次の写し:
//!
//! - `alc-misc` の repo/measurements.rs の `record_as_tenko_if_marked`・`record_or_existing_session_id`
//! - `alc-tenko` の normal_tenko.rs (`record`・`existing_session_id`・`carins_expiry`)
//!
//! 運行者端末は管理者ログイン無しで動くため、点呼の段 ([`super::flow`]) を踏まない。
//! 代わりに **測定の保存と同じ transaction** の中でセッションと記録を 1 組作る。記録の作成は内側の区切り
//! (SAVEPOINT) で行い、失敗したら内側だけ戻す — **測定の保存は必ず残す**。
//! 行の型・記録の書き方・hash は [`super::record`] (段の口・判定と同じもの)。

use alc_worker_db::TenantTx;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use super::record::{in_savepoint, status_for_result, write_record, Savepoint, TenkoSession};
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
            super::super::record::SESSION_COLUMNS
        )
    }

    /// 同じ測定に既にある、通常の流れ (通常点呼 / IT点呼) のセッションの id と点呼方法。述語は冪等用の部分 unique と同じで、
    /// 自動点呼・遠隔点呼が同じ測定を指している session は拾わない
    pub const EXISTING_SESSION: &str = "SELECT id, tenko_method FROM tenko_sessions \
         WHERE measurement_id = $1 AND tenant_id = $2 \
           AND (tenko_type = 'normal' OR tenko_method IN ('通常点呼', 'IT点呼')) \
         LIMIT 1";
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

    write_record(tx, &session).await?;
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
