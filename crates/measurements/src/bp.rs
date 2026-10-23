//! 血圧の LINE WORKS 通知 (本人宛。Refs ippoan/alc-app#436)。血圧が初めて保存された `PUT /measurements/{id}` の後に、
//! 測定した本人へ結果を LINE WORKS で送る。宛先は血圧の通知の設定の表 `bp_notify_settings` (alc-migrations 186。
//! 設定の口は [`crate::bp_settings`]) の、測定の乗務員の行 (`notify_self` かつ `lineworks_user_id` あり)。既存の通知の宛先は使わない。
//!
//! # 送る条件
//!
//! 保存が **status = completed で systolic が入り、更新前の systolic が NULL** のときだけ ([`should_notify`])。旧値は PUT の
//! transaction の中で `FOR UPDATE` で読む (アルコール検知メールと同じ。同じ測定への PUT が並んでも、条件を満たす保存は 1 つだけ)。
//! 血圧を保存し直しても、後から別の欄だけ保存しても二重には送らない。dev 端末の要求 (`X-Device-Dev: 1`) からは送らない。
//!
//! # 流れ (lock を握ったまま送らない)
//!
//! 1. 保存の transaction が commit される
//! 2. [`prepare`] — pg の lock を持ったまま、短い tenant_tx 1 回で宛先と基準値を引く。失敗しても保存は残る (ログ 1 行で送らない)
//! 3. 応答を返した後 — [`PendingBp`] を応答の extension に入れ、Worker の fetch が `wait_until` で [`PendingBp::send`] を走らせる。
//!    送信の失敗はログ 1 行 (段の名前か status だけ) で、保存の応答は変えない・リトライしない
//!
//! 送るのは ippoan/alc-lineworks-worker の `POST /api/internal/lineworks/messages` (`X-Tenant-ID`、`bot_config_id` は付けない =
//! tenant の既定の bot)。**ログは固定の語と件数だけ** (氏名・値・id・LINE WORKS の user_id を出さない)。

use alc_worker_db::{PgClient, TxOutput};
use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use serde_json::json;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::models::Measurement;
use crate::service::{ServiceRequest, SharedService};
use crate::{set_device_dev, AppState, LogSink};

/// lineworks-worker の送信の口 (alc-trouble-worker / alc-notify-worker と同じ)
pub const LW_MESSAGES_PATH: &str = "/api/internal/lineworks/messages";

/// 基準値の既定 (`employee_health_baselines` に行が無い乗務員。表の DEFAULT と同じ)
pub const DEFAULT_BASELINE: Baseline = Baseline {
    systolic: 120,
    diastolic: 80,
    systolic_tolerance: 10,
    diastolic_tolerance: 10,
};

/// 血圧の基準値と許容範囲 (±)。脈拍は判定しない
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Baseline {
    pub systolic: i32,
    pub diastolic: i32,
    pub systolic_tolerance: i32,
    pub diastolic_tolerance: i32,
}

impl TxOutput for Baseline {}

impl Baseline {
    /// 最高・最低のどちらかが 基準 ± 許容 を外れたら true (範囲の端は外れではない。来ていない値は見ない)
    pub fn is_off(&self, systolic: Option<i32>, diastolic: Option<i32>) -> bool {
        let off = |v: Option<i32>, base: i32, tol: i32| v.is_some_and(|v| (v - base).abs() > tol);
        off(systolic, self.systolic, self.systolic_tolerance)
            || off(diastolic, self.diastolic, self.diastolic_tolerance)
    }

    /// 基準値の列 (基準の最高・最低・許容の最高・最低) の行から。行が無ければ既定
    pub fn from_columns(row: Option<[Option<i32>; 4]>) -> Self {
        match row {
            Some([Some(systolic), Some(diastolic), Some(st), Some(dt)]) => Baseline {
                systolic,
                diastolic,
                systolic_tolerance: st,
                diastolic_tolerance: dt,
            },
            _ => DEFAULT_BASELINE,
        }
    }
}

/// 基準値の列 (表の NOT NULL の 4 列。行が無い乗務員は LEFT JOIN で全部 NULL)
pub const BASELINE_COLUMNS: &str =
    "b.baseline_systolic, b.baseline_diastolic, b.systolic_tolerance, b.diastolic_tolerance";

pub mod sql {
    /// 本人の宛先 (LINE WORKS の user_id)。血圧の通知の設定の表 `bp_notify_settings` (alc-migrations 186。1 社員 1 行) だけから引く。
    /// 本人への通知を切った (`notify_self` が false)・user_id が無い社員は引かない。既存の通知の宛先 (`notify_recipients`) は使わない
    pub const RECIPIENTS: &str = "SELECT lineworks_user_id FROM bp_notify_settings \
         WHERE tenant_id = $1 AND employee_id = $2 AND notify_self AND lineworks_user_id IS NOT NULL \
         ORDER BY lineworks_user_id";

    /// 乗務員の基準値 (1 乗務員 1 行。無ければ 0 行)
    pub const BASELINE: &str =
        "SELECT baseline_systolic, baseline_diastolic, systolic_tolerance, diastolic_tolerance \
         FROM employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2";
}

/// 日本時間 (+09:00)
pub fn jst() -> FixedOffset {
    FixedOffset::east_opt(9 * 3600).expect("+09:00")
}

/// `10/23 08:12` (日本時間。月日は 0 を埋めない)
pub fn month_day_time(t: DateTime<Utc>) -> String {
    let t = t.with_timezone(&jst());
    format!(
        "{}/{} {:02}:{:02}",
        t.month(),
        t.day(),
        t.hour(),
        t.minute()
    )
}

/// 初めて血圧が保存された保存か。`old_systolic` は更新前の値 (この保存で行を作る POST には無い = 対象外)、
/// `m` は更新後の行 (値を送らない PUT は COALESCE で旧値が残るので、入力ではなく更新後で見る)
pub fn should_notify(old_systolic: Option<i32>, m: &Measurement) -> bool {
    m.status == "completed" && m.systolic.is_some() && old_systolic.is_none()
}

/// 本文 (日本語。時刻は日本時間)。基準から外れていれば末尾に 1 行足す。脈拍が無ければその行は省く
pub fn compose(m: &Measurement, baseline: &Baseline) -> String {
    let at = month_day_time(m.medical_measured_at.unwrap_or(m.measured_at));
    let num = |v: Option<i32>| v.map_or_else(|| "-".to_owned(), |v| v.to_string());
    let mut text = format!(
        "血圧の記録 ({at})\n最高 {} / 最低 {} mmHg",
        num(m.systolic),
        num(m.diastolic)
    );
    if let Some(pulse) = m.pulse {
        text.push_str(&format!("\n脈拍 {pulse}"));
    }
    if baseline.is_off(m.systolic, m.diastolic) {
        text.push_str(&format!(
            "\n基準から外れています (基準 {}/{})",
            baseline.systolic, baseline.diastolic
        ));
    }
    text
}

/// lineworks-worker へ 1 人宛のテキストを送る要求 (`bot_config_id` は付けない = tenant の既定の bot)
pub fn message_request(tenant_id: Uuid, user_id: &str, text: &str) -> ServiceRequest {
    ServiceRequest {
        path: LW_MESSAGES_PATH.to_owned(),
        headers: vec![
            ("X-Tenant-ID", tenant_id.to_string()),
            ("Content-Type", "application/json".to_owned()),
        ],
        body: json!({
            "to": {"user_id": user_id},
            "content": {"type": "text", "text": text},
        })
        .to_string(),
    }
}

/// 宛先と基準値を、短い tenant_tx 1 回で引く
async fn lookup(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    employee_id: Uuid,
) -> Result<(Vec<String>, Baseline), tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args: [(&(dyn tokio_postgres::types::ToSql + Sync), Type); 2] =
                [(&tenant_id, Type::UUID), (&employee_id, Type::UUID)];
            let ids = tx
                .query_typed(sql::RECIPIENTS, &args)
                .await?
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            let baseline = tx
                .query_typed_opt(sql::BASELINE, &args)
                .await?
                .map(|r| [0, 1, 2, 3].map(|i| r.get::<_, Option<i32>>(i)));
            Ok((ids, Baseline::from_columns(baseline)))
        })
    })
    .await
}

/// 応答を返した後に送るもの。持つのは送り先の Worker と宛先・本文・ログだけで、**DB には触らない** (pg の lock を持たない)
#[derive(Clone)]
pub struct PendingBp {
    service: SharedService,
    tenant_id: Uuid,
    user_ids: Vec<String>,
    text: String,
    log: LogSink,
}

impl PendingBp {
    /// 宛先ごとに 1 通送る。失敗はログ 1 行 (届かない理由の label か status だけ) で、ほかの宛先へは続ける。
    /// 返すのは (送れた数, 失敗した数)
    pub async fn send(self) -> (usize, usize) {
        let (mut sent, mut failed) = (0, 0);
        for user_id in &self.user_ids {
            let req = message_request(self.tenant_id, user_id, &self.text);
            let why = match self.service.post(req).await {
                Ok(r) if (200..300).contains(&r.status) => {
                    sent += 1;
                    continue;
                }
                Ok(r) => format!("status {}", r.status),
                Err(e) => e.0.to_owned(),
            };
            failed += 1;
            (self.log)(&format!(
                "measurements: bp notify failed: lineworks ({why})"
            ));
        }
        (sent, failed)
    }
}

/// 保存が条件を満たした (`notify`) ときだけ、宛先を引いて送る用意をする。送らない理由はログ 1 行 (固定の語)。
/// dev 端末の要求・送り先の Worker が無い・宛先が無いときは送らない。**pg の lock を持ったまま呼び**、返した
/// [`PendingBp`] は応答の extension に入れて応答の後に送る。引きの失敗は保存を巻き戻さない
pub async fn prepare(
    state: &AppState,
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    notify: bool,
    m: &Measurement,
) -> Option<PendingBp> {
    if !notify {
        return None;
    }
    if dev {
        (state.log)("measurements: bp notify skipped: dev");
        return None;
    }
    let Some(service) = state.lineworks.clone() else {
        (state.log)("measurements: bp notify skipped: no lineworks");
        return None;
    };
    let (user_ids, baseline) = match lookup(pg, tenant_id, dev, m.employee_id).await {
        Ok(found) => found,
        Err(e) => {
            // 保存はもう commit 済み。ログだけ残して送らない
            state.db_fail("bp notify lookup", &e);
            return None;
        }
    };
    if user_ids.is_empty() {
        (state.log)("measurements: bp notify skipped: no recipient");
        return None;
    }
    Some(PendingBp {
        service,
        tenant_id,
        user_ids,
        text: compose(m, &baseline),
        log: state.log.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn measurement(status: &str, systolic: Option<i32>) -> Measurement {
        let t = at("2026-10-22T23:12:00Z");
        Measurement {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            employee_id: Uuid::nil(),
            alcohol_level: None,
            result: None,
            device_use_count: 0,
            face_photo_url: None,
            video_url: None,
            measured_at: t,
            created_at: t,
            updated_at: t,
            status: status.into(),
            temperature: None,
            systolic,
            diastolic: Some(78),
            pulse: Some(66),
            medical_measured_at: Some(t),
            face_verified: None,
            medical_manual_input: None,
        }
    }

    #[test]
    fn notify_only_when_a_completed_save_first_brings_systolic() {
        let done = measurement("completed", Some(124));
        assert!(should_notify(None, &done));
        // 旧が入っていれば (2 回目以降の保存) 送らない
        assert!(!should_notify(Some(120), &done));
        // 完了でない・血圧が無い保存は送らない
        assert!(!should_notify(None, &measurement("started", Some(124))));
        assert!(!should_notify(None, &measurement("completed", None)));
    }

    #[test]
    fn baseline_edges_are_not_off() {
        let b = DEFAULT_BASELINE;
        assert!(!b.is_off(Some(130), Some(70)));
        assert!(!b.is_off(Some(110), Some(90)));
        assert!(b.is_off(Some(131), Some(80)));
        assert!(b.is_off(Some(109), Some(80)));
        assert!(b.is_off(Some(120), Some(91)));
        assert!(b.is_off(Some(120), Some(69)));
        // 来ていない値は見ない
        assert!(!b.is_off(None, None));
        assert!(b.is_off(None, Some(95)));
    }

    #[test]
    fn baseline_from_columns_falls_back_to_the_default() {
        let own = Baseline::from_columns(Some([Some(150), Some(95), Some(5), Some(6)]));
        assert_eq!(
            own,
            Baseline {
                systolic: 150,
                diastolic: 95,
                systolic_tolerance: 5,
                diastolic_tolerance: 6
            }
        );
        // 行が無い (LEFT JOIN で全部 NULL を含む) 乗務員は既定
        assert_eq!(Baseline::from_columns(None), DEFAULT_BASELINE);
        assert_eq!(
            Baseline::from_columns(Some([None, None, None, None])),
            DEFAULT_BASELINE
        );
    }

    #[test]
    fn compose_uses_japan_time_and_skips_missing_lines() {
        let m = measurement("completed", Some(124));
        assert_eq!(
            compose(&m, &DEFAULT_BASELINE),
            "血圧の記録 (10/23 08:12)\n最高 124 / 最低 78 mmHg\n脈拍 66"
        );
        // 血圧の時刻が無ければ測定の時刻。最低・脈拍が無ければ - と省略。基準外は 1 行足す
        let mut m = measurement("completed", Some(150));
        m.medical_measured_at = None;
        m.measured_at = at("2026-01-02T00:05:00Z");
        m.diastolic = None;
        m.pulse = None;
        assert_eq!(
            compose(&m, &DEFAULT_BASELINE),
            "血圧の記録 (1/2 09:05)\n最高 150 / 最低 - mmHg\n基準から外れています (基準 120/80)"
        );
    }

    #[test]
    fn message_request_has_no_bot_config_id() {
        let tenant = Uuid::nil();
        let req = message_request(tenant, "u1", "本文");
        assert_eq!(req.path, "/api/internal/lineworks/messages");
        assert_eq!(
            req.headers,
            [
                ("X-Tenant-ID", tenant.to_string()),
                ("Content-Type", "application/json".to_owned())
            ]
        );
        let v: serde_json::Value = serde_json::from_str(&req.body).unwrap();
        assert_eq!(
            v,
            json!({"to": {"user_id": "u1"}, "content": {"type": "text", "text": "本文"}})
        );
    }
}
