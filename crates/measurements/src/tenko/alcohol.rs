//! 自動点呼・遠隔点呼のアルコールの段 (Refs ippoan/rust-alc-api#680)。
//!
//! **測定の保存で進める** ([`lock_for_measurement`]・[`advance`]): キオスクは測定の完了の `PUT /measurements/{id}` に
//! `tenko_session_id` を付ける。同じ transaction で、本人確認済み (`identity_verified`) のセッションを **測定の行の**
//! `result`・`alcohol_level`・`face_photo_url`・`measured_at` から進める (クライアントの本文のアルコールの結果を信じない)。
//! 検知 (`fail` / `over`) なら中止で閉じて記録を書き、webhook `alcohol_detected` を渡す。進めるのは内側の区切りの中で、
//! 失敗しても測定の保存は残す。
//!
//! アルコールの結果を本文で受ける段の口 (`PUT /tenko/sessions/{id}/alcohol`) は消した (alc-app のキオスクが測定の保存に移った)。

use alc_worker_db::TenantTx;
use chrono::{DateTime, Utc};
use const_format::concatcp;
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use super::fire::Event;
use super::flow::{fetch, one};
use super::record::{
    employee_name, in_savepoint, record_or_log, status_for_result, Savepoint, TenkoSession,
    SESSION_COLUMNS,
};
use crate::models::Measurement;
use crate::LogSink;

/// アルコールの段を進められる status
pub const ALCOHOL_STATUS: &str = "identity_verified";

/// 測定の保存で段を進める区切り (失敗しても測定の保存は残す)
pub const SP_ALCOHOL: Savepoint = Savepoint {
    begin: "SAVEPOINT tenko_alcohol",
    release: "RELEASE SAVEPOINT tenko_alcohol",
    rollback: "ROLLBACK TO SAVEPOINT tenko_alcohol",
};

pub mod sql {
    use super::*;

    /// $1 status / $2 measurement_id / $3 alcohol_result / $4 alcohol_value / $5 alcohol_face_photo_url /
    /// $6 cancel_reason / $7 completed_at / $8 alcohol_tested_at (測定時刻) / $9 id / $10 tenant_id
    pub const UPDATE_ALCOHOL: &str = concatcp!(
        "UPDATE alc_api.tenko_sessions SET status = $1, measurement_id = $2, alcohol_result = $3, alcohol_value = $4, \
             alcohol_tested_at = $8, alcohol_face_photo_url = $5, cancel_reason = $6, completed_at = $7, \
             updated_at = NOW() \
         WHERE id = $9 AND tenant_id = $10 RETURNING ",
        SESSION_COLUMNS
    );
}

/// アルコールの段の次 (検知なら中止。業務前は指示の確認、業務後は運行報告)。種別が業務前・業務後でなければ `None`
fn next_status(cancelled: bool, tenko_type: &str) -> Option<&'static str> {
    if cancelled {
        return Some("cancelled");
    }
    match tenko_type {
        "pre_operation" => Some("instruction_pending"),
        "post_operation" => Some("report_pending"),
        _ => None,
    }
}

/// 段を進めて、検知なら記録と webhook を作る
#[allow(clippy::too_many_arguments)]
async fn apply(
    tx: &TenantTx<'_>,
    cur: &TenkoSession,
    next: &str,
    measurement_id: Uuid,
    result: &str,
    value: Option<f64>,
    face_photo_url: &Option<String>,
    tested_at: DateTime<Utc>,
    at: DateTime<Utc>,
    log: &LogSink,
) -> Result<(TenkoSession, Option<Event>), Error> {
    let judged = status_for_result(Some(result));
    let cancel_reason = judged.and_then(|(_, r)| r).map(str::to_owned);
    let is_fail = next == "cancelled";
    let completed_at = is_fail.then_some(at);
    let s = one(
        tx,
        sql::UPDATE_ALCOHOL,
        &[
            (&next, Type::TEXT),
            (&measurement_id, Type::UUID),
            (&result, Type::TEXT),
            (&value, Type::FLOAT8),
            (face_photo_url, Type::TEXT),
            (&cancel_reason, Type::TEXT),
            (&completed_at, Type::TIMESTAMPTZ),
            (&tested_at, Type::TIMESTAMPTZ),
            (&cur.id, Type::UUID),
            (&cur.tenant_id, Type::UUID),
        ],
    )
    .await?;
    if !is_fail {
        return Ok((s, None));
    }
    record_or_log(tx, &s, log).await;
    let name = employee_name(tx, s.tenant_id, s.employee_id).await?;
    let payload = json!({
        "event": "alcohol_detected",
        "timestamp": at,
        "tenant_id": s.tenant_id,
        "data": {
            "session_id": s.id,
            "employee_id": s.employee_id,
            "employee_name": name.unwrap_or_default(),
            "alcohol_value": value,
            "alcohol_result": result,
            "responsible_manager_name": s.responsible_manager_name,
            "tenko_type": s.tenko_type,
        }
    });
    Ok((s, Some(Event::new("alcohol_detected", payload))))
}

/// 測定の保存に付いた点呼セッションを押さえる (測定の UPDATE の前に呼ぶ)。
///
/// * 無い (別のテナント・dev の軸が違うものを含む)・乗務員が測定と違う → `None` (保存ごと 409 にする)
/// * 本人確認済み、または **この測定で既に進めた** (再送) → `Some`
/// * それ以外の status → `None`
pub async fn lock_for_measurement(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    session_id: Uuid,
    measurement_id: Uuid,
    employee_id: Uuid,
) -> Result<Option<TenkoSession>, Error> {
    let Some(cur) = fetch(tx, tenant_id, session_id).await? else {
        return Ok(None);
    };
    let ready = cur.status == ALCOHOL_STATUS || cur.measurement_id == Some(measurement_id);
    Ok((cur.employee_id == employee_id && ready).then_some(cur))
}

/// 保存した測定の行で段を進める ([`lock_for_measurement`] が押さえたセッション)。進めたセッション (進めなかったら元のまま) と、
/// 検知なら webhook を返す。
///
/// 進めないのは: 測定が未完了・この測定で既に進めた (再送)・結果が点呼にならない (`error` / 無し。本人確認済みのまま残り、
/// 測り直せる)・種別が業務前・業務後でない・区切りの中で失敗した (ログ 1 行。測定の保存は残す)
pub async fn advance(
    tx: &TenantTx<'_>,
    cur: TenkoSession,
    m: &Measurement,
    at: DateTime<Utc>,
    log: &LogSink,
) -> (TenkoSession, Option<Event>) {
    if m.status != "completed" || cur.status != ALCOHOL_STATUS {
        return (cur, None);
    }
    let result = m.result.as_deref();
    let (Some(result), Some((closed, _))) = (result, status_for_result(result)) else {
        return (cur, None);
    };
    let Some(next) = next_status(closed == "cancelled", &cur.tenko_type) else {
        return (cur, None);
    };
    let body = apply(
        tx,
        &cur,
        next,
        m.id,
        result,
        m.alcohol_level,
        &m.face_photo_url,
        m.measured_at,
        at,
        log,
    );
    match in_savepoint(tx, &SP_ALCOHOL, body).await {
        Ok(done) => done,
        Err(stage) => {
            log(&format!("measurements: tenko alcohol failed: {stage}"));
            (cur, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_status_by_type() {
        assert_eq!(next_status(true, "pre_operation"), Some("cancelled"));
        assert_eq!(next_status(true, "normal"), Some("cancelled"));
        assert_eq!(
            next_status(false, "pre_operation"),
            Some("instruction_pending")
        );
        assert_eq!(next_status(false, "post_operation"), Some("report_pending"));
        assert_eq!(next_status(false, "normal"), None);
    }
}
