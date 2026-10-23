//! 毎朝 7:00 (日本時間) の「血圧の日報」メール (Refs ippoan/alc-app#436)。前日 (日本時間 0:00〜24:00) に血圧を測った全員の
//! 記録の一覧を、テナントの**管理者**へ 1 人 1 通送る (既定で送り、外したい人だけ外すオプトアウト)。
//! 宛先は `notify_recipients` のうち `enabled`・email あり・結び付いた社員 (`employee_id`。削除済みでない) の `role` に
//! `manager` か `admin` を含み、notify のグループ「血圧の日報を受け取らない」のメンバーでない人 ([`sql::RECIPIENTS`])。
//! 送信はアルコール検知メールと同じ ([`crate::alert::deliver`])。旧グループ「血圧の日報」は使わない (残っていても無視)。
//!
//! worker の定時実行 (wrangler.toml の `[triggers]`。本番だけ [`DAILY_CRON`]) が [`run`] を呼ぶ。
//!
//! # テナントごと
//!
//! テナントの一覧は `tenants` を nil の tenant_tx で読む。1 テナントずつ短い tenant_tx 1 回で (宛先, 前日の記録) を引き、
//! 宛先が 0 (管理者がいない・全員が外した・メールが使えない) のテナントは何もしない。記録が 0 件なら「記録はありません」と送る。
//! 1 テナントの失敗で残りを止めない (リトライしない)。dev 端末の記録は RLS が分けるので入らない。
//!
//! 記録の時刻は `medical_measured_at` (無ければ `measured_at`)。完了 (completed) で systolic が入っている行だけ。
//! **ログは件数だけ** (氏名・値・id・メールアドレスを出さない)。

use alc_worker_db::{kind, PgClient, TxOutput};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::alert::{self, Outbox, SharedMailer, UNKNOWN_EMPLOYEE};
use crate::bp::{self, Baseline, BASELINE_COLUMNS};
use crate::LogSink;

/// 日報を受け取らない人のグループの名前 (テナントごとに notify の画面で作る。メンバーは宛先から外れる)
pub const OPT_OUT_GROUP: &str = "血圧の日報を受け取らない";

/// 日報の定時実行 (UTC。毎日 22:00 = 日本時間の翌朝 7:00)。wrangler.toml の本番の `[triggers]` と同じ文字列
pub const DAILY_CRON: &str = "0 22 * * *";

pub mod sql {
    use super::BASELINE_COLUMNS;

    /// テナントの一覧 (`tenants` は RLS が無く、実行用ロールが読める)
    pub const TENANTS: &str = "SELECT id FROM tenants ORDER BY id";

    /// 日報の宛先の email。$1 = tenant_id、$2 = 受け取らないグループの名前。
    /// 使える宛先 (`enabled`・email あり) のうち、結び付いた社員 (削除済みでない) が manager か admin で、
    /// 受け取らないグループのメンバーでない人。WHERE に tenant_id も明示する
    pub const RECIPIENTS: &str = "SELECT r.email \
         FROM notify_recipients r \
         JOIN employees e ON e.id = r.employee_id AND e.tenant_id = r.tenant_id \
         WHERE r.tenant_id = $1 AND r.enabled AND r.email IS NOT NULL AND btrim(r.email) <> '' \
           AND e.deleted_at IS NULL AND e.role && ARRAY['manager', 'admin']::text[] \
           AND NOT EXISTS ( \
               SELECT 1 FROM notify_recipient_groups m \
               JOIN notify_groups g ON g.id = m.group_id AND g.tenant_id = $1 \
               WHERE m.recipient_id = r.id AND g.name = $2)";

    /// 前日の記録 (時刻順)。基準値は LEFT JOIN (無ければ全部 NULL = 既定)。WHERE に tenant_id も明示する
    pub fn records() -> String {
        format!(
            "SELECT e.name, COALESCE(m.medical_measured_at, m.measured_at) AS at, \
                    m.systolic, m.diastolic, m.pulse, {BASELINE_COLUMNS} \
             FROM measurements m \
             LEFT JOIN employees e ON e.id = m.employee_id AND e.tenant_id = m.tenant_id \
             LEFT JOIN employee_health_baselines b \
                    ON b.employee_id = m.employee_id AND b.tenant_id = m.tenant_id \
             WHERE m.tenant_id = $1 AND m.status = 'completed' AND m.systolic IS NOT NULL \
               AND COALESCE(m.medical_measured_at, m.measured_at) >= $2 \
               AND COALESCE(m.medical_measured_at, m.measured_at) < $3 \
             ORDER BY at, e.name"
        )
    }
}

/// 日報の 1 行
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub name: String,
    pub at: DateTime<Utc>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub off_baseline: bool,
}

impl TxOutput for Record {}

/// `now` (UTC) が日本時間で何日かを見て、その前日の (始め, 終わり, 日付)。始め以上・終わり未満が前日の 0:00〜24:00
pub fn yesterday(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>, NaiveDate) {
    let jst = bp::jst();
    let today = now.with_timezone(&jst).date_naive();
    let day = today - Duration::days(1);
    let midnight = |d: NaiveDate| {
        jst.from_local_datetime(&d.and_hms_opt(0, 0, 0).expect("0:00"))
            .single()
            .expect("固定の offset に曖昧な時刻は無い")
            .with_timezone(&Utc)
    };
    (midnight(day), midnight(today), day)
}

/// 定時実行の時刻 (Cloudflare の `scheduled_time`。epoch ミリ秒) から
pub fn from_millis(ms: f64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms as i64)
}

fn single_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

fn line(r: &Record) -> String {
    let num = |v: Option<i32>| v.map_or_else(|| "-".to_owned(), |v| v.to_string());
    let hm = bp::month_day_time(r.at);
    // 日付は件名にあるので時刻だけ (`10/22 08:12` の空白より後ろ)
    let time = hm.split_once(' ').map_or(hm.as_str(), |(_, t)| t);
    format!(
        "{}  {}  {}/{}  {}",
        single_line(&r.name),
        time,
        num(r.systolic),
        num(r.diastolic),
        num(r.pulse)
    )
}

/// 件名と本文 (text)。基準から外れた記録を先頭にまとめ、そのあとに全員の記録。記録が無ければ「記録はありません」
pub fn compose(day: NaiveDate, records: &[Record]) -> (String, String) {
    use chrono::Datelike;
    let label = format!("{}/{}", day.month(), day.day());
    let subject = format!("血圧の日報 ({label})");
    if records.is_empty() {
        let body = format!("{subject}\n\n記録はありません\n");
        return (subject, body);
    }
    let header = "氏名  時刻  最高/最低 (mmHg)  脈拍";
    let off: Vec<&Record> = records.iter().filter(|r| r.off_baseline).collect();
    let mut body = format!(
        "{subject}\n測定 {} 件 / 基準から外れた記録 {} 件\n\n",
        records.len(),
        off.len()
    );
    body.push_str("【基準から外れた記録】\n");
    if off.is_empty() {
        body.push_str("なし\n");
    } else {
        body.push_str(&format!("{header}\n"));
        for r in off {
            body.push_str(&format!("{}\n", line(r)));
        }
    }
    body.push_str(&format!("\n【全員の記録】\n{header}\n"));
    for r in records {
        body.push_str(&format!("{}\n", line(r)));
    }
    (subject, body)
}

/// 1 回の実行の件数 (ログに出すのは件数だけ)
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReportSummary {
    /// テナントの数
    pub tenants: usize,
    /// 宛先が無く、送らなかったテナント
    pub skipped: usize,
    /// 引きに失敗したテナント
    pub lookup_failed: usize,
    /// 送れた通数
    pub sent: usize,
    /// 送れなかった通数
    pub failed: usize,
}

impl ReportSummary {
    /// ログの 1 行 (件数だけ)
    pub fn line(&self) -> String {
        format!(
            "measurements: bp report: tenants {} skipped {} lookup_failed {} sent {} failed {}",
            self.tenants, self.skipped, self.lookup_failed, self.sent, self.failed
        )
    }
}

/// 日報の宛先の email (昇順・重複なし)
async fn recipient_emails(
    tx: &alc_worker_db::TenantTx<'_>,
    tenant_id: Uuid,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::RECIPIENTS,
            &[(&tenant_id, Type::UUID), (&OPT_OUT_GROUP, Type::TEXT)],
        )
        .await?;
    let mut emails: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    // 同じ email の宛先が 2 つ在っても 1 通
    emails.sort();
    emails.dedup();
    Ok(emails)
}

/// 1 テナントの (宛先の email, 記録)
async fn lookup(
    pg: &mut PgClient,
    tenant_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<(Vec<String>, Vec<Record>), tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let emails = recipient_emails(tx, tenant_id).await?;
            if emails.is_empty() {
                // 宛先が無ければ記録は引かない
                return Ok((emails, Vec::new()));
            }
            let rows = tx
                .query_typed(
                    &sql::records(),
                    &[
                        (&tenant_id, Type::UUID),
                        (&from, Type::TIMESTAMPTZ),
                        (&to, Type::TIMESTAMPTZ),
                    ],
                )
                .await?;
            let records = rows
                .iter()
                .map(|r| {
                    let baseline = Baseline::from_columns(Some(
                        [5, 6, 7, 8].map(|i| r.get::<_, Option<i32>>(i)),
                    ));
                    let (systolic, diastolic) = (r.get(2), r.get(3));
                    Record {
                        name: r
                            .get::<_, Option<String>>(0)
                            .unwrap_or_else(|| UNKNOWN_EMPLOYEE.to_owned()),
                        at: r.get(1),
                        systolic,
                        diastolic,
                        pulse: r.get(4),
                        off_baseline: baseline.is_off(systolic, diastolic),
                    }
                })
                .collect();
            Ok((emails, records))
        })
    })
    .await
}

/// 前日の日報を、宛先のあるテナントごとに送る。送り手が無い・テナントの一覧が取れなければ `None` (理由はログ 1 行)
pub async fn run(
    pg: &mut PgClient,
    mailer: Option<SharedMailer>,
    log: &LogSink,
    now: DateTime<Utc>,
) -> Option<ReportSummary> {
    let Some(mailer) = mailer else {
        log("measurements: bp report skipped: no mailer");
        return None;
    };
    let listed = pg
        .tenant_tx(Uuid::nil(), move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(sql::TENANTS, &[]).await?;
                Ok(rows.iter().map(|r| r.get::<_, Uuid>(0)).collect::<Vec<_>>())
            })
        })
        .await;
    let tenants = match listed {
        Ok(t) => t,
        Err(e) => {
            log(&format!(
                "measurements: bp report list failed: db ({})",
                kind(&e)
            ));
            return None;
        }
    };
    let (from, to, day) = yesterday(now);
    let mut summary = ReportSummary {
        tenants: tenants.len(),
        ..Default::default()
    };
    for tenant_id in tenants {
        let (emails, records) = match lookup(pg, tenant_id, from, to).await {
            Ok(found) => found,
            Err(e) => {
                log(&format!(
                    "measurements: bp report lookup failed: db ({})",
                    kind(&e)
                ));
                summary.lookup_failed += 1;
                continue;
            }
        };
        if emails.is_empty() {
            summary.skipped += 1;
            continue;
        }
        let (subject, body) = compose(day, &records);
        let (sent, failed) =
            alert::deliver(Outbox::new(mailer.clone(), emails, &subject, &body), log).await;
        summary.sent += sent;
        summary.failed += failed;
    }
    Some(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn record(name: &str, time: &str, off: bool) -> Record {
        Record {
            name: name.into(),
            at: at(time),
            systolic: Some(140),
            diastolic: Some(88),
            pulse: Some(70),
            off_baseline: off,
        }
    }

    #[test]
    fn yesterday_is_the_previous_japan_day() {
        // 毎朝 7:00 (日本時間) = 前日の 22:00 UTC。10/23 7:00 に動けば 10/22 の 0:00〜24:00 (UTC で 10/21 15:00〜10/22 15:00)
        let (from, to, day) = yesterday(at("2026-10-22T22:00:00Z"));
        assert_eq!(from, at("2026-10-21T15:00:00Z"));
        assert_eq!(to, at("2026-10-22T15:00:00Z"));
        assert_eq!(day.to_string(), "2026-10-22");
        // 日本時間の日付で決める: UTC ではまだ前日の 14:59 でも、日本時間は日付が替わった後
        let (from, _, day) = yesterday(at("2026-10-22T15:00:00Z"));
        assert_eq!(from, at("2026-10-21T15:00:00Z"));
        assert_eq!(day.to_string(), "2026-10-22");
        let (_, to, day) = yesterday(at("2026-10-22T14:59:59Z"));
        assert_eq!(to, at("2026-10-21T15:00:00Z"));
        assert_eq!(day.to_string(), "2026-10-21");
        // 年・月の境
        assert_eq!(
            yesterday(at("2026-12-31T22:00:00Z")).2.to_string(),
            "2026-12-31"
        );
        assert_eq!(
            yesterday(at("2026-12-31T14:59:00Z")).2.to_string(),
            "2026-12-30"
        );
    }

    #[test]
    fn from_millis_reads_the_scheduled_time() {
        assert_eq!(
            from_millis(1_790_114_400_000.0),
            Some(at("2026-09-22T22:00:00Z"))
        );
        assert_eq!(from_millis(f64::MAX), None);
    }

    #[test]
    fn compose_without_records_says_so() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 22).unwrap();
        let (subject, body) = compose(day, &[]);
        assert_eq!(subject, "血圧の日報 (10/22)");
        assert_eq!(body, "血圧の日報 (10/22)\n\n記録はありません\n");
    }

    #[test]
    fn compose_puts_off_baseline_first_then_everyone() {
        let day = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let records = [
            record("山田\n太郎", "2026-01-04T23:12:00Z", false),
            record("鈴木", "2026-01-05T00:30:00Z", true),
        ];
        let (subject, body) = compose(day, &records);
        assert_eq!(subject, "血圧の日報 (1/5)");
        assert_eq!(
            body,
            "血圧の日報 (1/5)\n測定 2 件 / 基準から外れた記録 1 件\n\n\
             【基準から外れた記録】\n氏名  時刻  最高/最低 (mmHg)  脈拍\n鈴木  09:30  140/88  70\n\n\
             【全員の記録】\n氏名  時刻  最高/最低 (mmHg)  脈拍\n山田 太郎  08:12  140/88  70\n鈴木  09:30  140/88  70\n"
        );
    }

    #[test]
    fn compose_with_nobody_off_and_missing_values() {
        let day = NaiveDate::from_ymd_opt(2026, 10, 22).unwrap();
        let mut r = record("山田", "2026-10-21T15:00:00Z", false);
        r.diastolic = None;
        r.pulse = None;
        let (_, body) = compose(day, &[r]);
        assert!(body.contains("【基準から外れた記録】\nなし\n"), "{body}");
        assert!(body.contains("山田  00:00  140/-  -\n"), "{body}");
    }

    #[test]
    fn summary_line_has_counts_only() {
        let s = ReportSummary {
            tenants: 3,
            skipped: 1,
            lookup_failed: 1,
            sent: 4,
            failed: 2,
        };
        assert_eq!(
            s.line(),
            "measurements: bp report: tenants 3 skipped 1 lookup_failed 1 sent 4 failed 2"
        );
    }
}
