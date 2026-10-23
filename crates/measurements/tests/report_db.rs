//! 毎朝 7:00 (日本時間) の「血圧の日報」メール (Refs ippoan/alc-app#436) を実 DB まで通す。送り手は `MockMailer`。
//!
//! 前日 (日本時間 0:00〜24:00) に血圧を測った全員の一覧を、テナントの管理者 (削除済みでない社員で role に manager か admin) のうち、
//! 血圧の通知の設定 (`bp_notify_settings`。alc-migrations 186) で `daily_report` が true・email がある人へ 1 人 1 通。
//! 既存の通知の宛先 (`notify_recipients`)・notify のグループは使わない。テナントごとに引く。**テストの DB は他のテストと共有で、`run` は全テナントを回る**ので、
//! 件数は見ずに、このテストの宛先 (乱数のドメイン) に届いた分だけを見る。

mod support;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use measurements::report::run;
use support::*;
use uuid::Uuid;

/// 10/23 7:00 (日本時間) に動く定時実行 = 対象は 10/22
const NOW: &str = "2026-10-22T22:00:00Z";

fn now() -> DateTime<Utc> {
    NOW.parse().unwrap()
}

/// 他のテストの宛先と混ざらない乱数のドメイン
fn domain() -> String {
    format!("{}.example.test", Uuid::new_v4().simple())
}

/// 管理者などの社員を 1 人作り (`role`。`deleted` なら削除済み)、`settings` なら血圧の通知の設定の行 (email と `daily_report`) も作る
/// (superuser で)。`settings` が false なら設定の行は無い。社員の id を返す
async fn recipient(
    tenant: Uuid,
    email: Option<&str>,
    daily_report: bool,
    settings: bool,
    role: &[&str],
    deleted: bool,
) -> Uuid {
    let admin = admin().await;
    let role: Vec<String> = role.iter().map(|r| (*r).to_owned()).collect();
    let id = Uuid::new_v4();
    admin
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, nfc_id, name, code, role, deleted_at) \
             VALUES ($1, $2, $3, '宛先の社員', $4, $5, CASE WHEN $6 THEN now() END)",
            &[
                &id,
                &tenant,
                &id.to_string(),
                &id.simple().to_string()[..8].to_owned(),
                &role,
                &deleted,
            ],
        )
        .await
        .unwrap();
    if settings {
        admin
            .execute(
                "INSERT INTO alc_api.bp_notify_settings (tenant_id, employee_id, email, daily_report) \
                 VALUES ($1, $2, $3, $4)",
                &[&tenant, &id, &email, &daily_report],
            )
            .await
            .unwrap();
    }
    id
}

/// テナントと、`names` の管理者 (社員の role = admin・設定の行で email あり・日報を受け取る)
async fn setup(domain: &str, names: &[&str]) -> Uuid {
    let tenant = tenant().await;
    for n in names {
        recipient(
            tenant,
            Some(&format!("{n}@{domain}")),
            true,
            true,
            &["admin"],
            false,
        )
        .await;
    }
    tenant
}

async fn emp(tenant: Uuid, name: &str) -> Uuid {
    employee(tenant, name, &Uuid::new_v4().simple().to_string()[..8]).await
}

/// 測定を 1 件入れる (superuser で)。`medical_at` が無ければ `measured_at` が血圧の時刻になる
async fn record(
    tenant: Uuid,
    emp: Uuid,
    status: &str,
    systolic: Option<i32>,
    medical_at: Option<&str>,
    measured_at: &str,
) {
    let medical: Option<DateTime<Utc>> = medical_at.map(|s| s.parse().unwrap());
    let measured: DateTime<Utc> = measured_at.parse().unwrap();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.measurements \
                 (tenant_id, employee_id, status, systolic, diastolic, pulse, medical_measured_at, measured_at) \
             VALUES ($1, $2, $3, $4, 80, 64, $5, $6)",
            &[&tenant, &emp, &status, &systolic, &medical, &measured],
        )
        .await
        .unwrap();
}

async fn baseline(tenant: Uuid, emp: Uuid, systolic: i32, diastolic: i32, tol: i32) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employee_health_baselines \
                 (tenant_id, employee_id, baseline_systolic, baseline_diastolic, systolic_tolerance, diastolic_tolerance) \
             VALUES ($1, $2, $3, $4, $5, $5)",
            &[&tenant, &emp, &systolic, &diastolic, &tol],
        )
        .await
        .unwrap();
}

/// このテストの宛先 (`domain`) に届いた分
fn mine(mailer: &MockMailer, domain: &str) -> Vec<measurements::alert::AlertMail> {
    mailer
        .sent()
        .into_iter()
        .filter(|m| m.to.ends_with(&format!("@{domain}")))
        .collect()
}

fn logs_sink() -> (measurements::LogSink, Arc<std::sync::Mutex<Vec<String>>>) {
    let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = logs.clone();
    (
        Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        logs,
    )
}

#[tokio::test]
async fn test_sends_yesterdays_records_with_off_baseline_first() {
    let d = domain();
    let tenant = setup(&d, &["b", "a"]).await;
    // 入らない人: 管理者でない・設定の行なし・email なし・日報を受け取らない・削除済み
    let off = |n: &str| Some(format!("{n}@{d}"));
    recipient(
        tenant,
        off("driver").as_deref(),
        true,
        true,
        &["driver"],
        false,
    )
    .await;
    recipient(tenant, off("nolink").as_deref(), true, false, &[], false).await;
    recipient(tenant, None, true, true, &["admin"], false).await;
    recipient(
        tenant,
        off("disabled").as_deref(),
        false,
        true,
        &["admin"],
        false,
    )
    .await;
    recipient(
        tenant,
        off("deleted").as_deref(),
        true,
        true,
        &["admin"],
        true,
    )
    .await;
    recipient(
        tenant,
        off("optout").as_deref(),
        false,
        true,
        &["manager"],
        false,
    )
    .await;
    // manager も入る (役割が複数でもよい)
    recipient(
        tenant,
        off("mgr").as_deref(),
        true,
        true,
        &["driver", "manager"],
        false,
    )
    .await;

    let (yamada, suzuki, edge_start, edge_end) = (
        emp(tenant, "山田").await,
        emp(tenant, "鈴木").await,
        emp(tenant, "前日の始め").await,
        emp(tenant, "前日の終わり").await,
    );
    let (before, after, started, no_bp) = (
        emp(tenant, "前々日").await,
        emp(tenant, "当日").await,
        emp(tenant, "未完了").await,
        emp(tenant, "血圧なし").await,
    );
    // 山田: 08:12 (日本時間)。基準の既定 120/80 ±10 に収まる (systolic 125)
    record(
        tenant,
        yamada,
        "completed",
        Some(125),
        Some("2026-10-21T23:12:00Z"),
        "2026-10-21T23:12:00Z",
    )
    .await;
    // 鈴木: 医療の時刻が無いので measured_at (10/22 18:30)。収まらない (150) が、この人の基準 150/80 ±10 では外れない
    record(
        tenant,
        suzuki,
        "completed",
        Some(150),
        None,
        "2026-10-22T09:30:00Z",
    )
    .await;
    baseline(tenant, suzuki, 150, 80, 10).await;
    // 前日の境: 10/22 0:00 ちょうど (含む) と 23:59:59 (含む)。外れる値 (160)
    record(
        tenant,
        edge_start,
        "completed",
        Some(160),
        Some("2026-10-21T15:00:00Z"),
        "2026-10-21T15:00:00Z",
    )
    .await;
    record(
        tenant,
        edge_end,
        "completed",
        Some(160),
        Some("2026-10-22T14:59:59Z"),
        "2026-10-22T14:59:59Z",
    )
    .await;
    // 前日の外: 10/21 23:59:59 と 10/23 0:00 ちょうど。完了でない・血圧のない記録
    record(
        tenant,
        before,
        "completed",
        Some(160),
        Some("2026-10-21T14:59:59Z"),
        "2026-10-21T14:59:59Z",
    )
    .await;
    record(
        tenant,
        after,
        "completed",
        Some(160),
        Some("2026-10-22T15:00:00Z"),
        "2026-10-22T15:00:00Z",
    )
    .await;
    record(
        tenant,
        started,
        "started",
        Some(160),
        Some("2026-10-22T01:00:00Z"),
        "2026-10-22T01:00:00Z",
    )
    .await;
    record(
        tenant,
        no_bp,
        "completed",
        None,
        Some("2026-10-22T01:00:00Z"),
        "2026-10-22T01:00:00Z",
    )
    .await;

    let mailer = Arc::new(MockMailer::default());
    let (log, logs) = logs_sink();
    let mut pg = rt_client().await;
    let summary = run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap();

    assert!(summary.sent >= 2, "{summary:?}");
    let sent = mine(&mailer, &d);
    // 管理者 a・b と manager の mgr だけ。入らない人 (driver・設定の行なし・email なし・日報を受け取らない・削除済み) には届かない
    let mut to: Vec<_> = sent.iter().map(|m| m.to.clone()).collect();
    to.sort();
    assert_eq!(
        to,
        [format!("a@{d}"), format!("b@{d}"), format!("mgr@{d}")],
        "{sent:?}"
    );
    for mail in &sent {
        assert_eq!(mail.subject, "血圧の日報 (10/22)");
        assert_eq!(mail.attachment, None);
        assert_eq!(mail.body, sent[0].body);
    }
    // 基準から外れた 2 人 (前日の始め・終わり) が先頭の節、そのあと全員 (時刻順)
    assert_eq!(
        sent[0].body,
        "血圧の日報 (10/22)\n測定 4 件 / 基準から外れた記録 2 件\n\n\
         【基準から外れた記録】\n氏名  時刻  最高/最低 (mmHg)  脈拍\n\
         前日の始め  00:00  160/80  64\n前日の終わり  23:59  160/80  64\n\n\
         【全員の記録】\n氏名  時刻  最高/最低 (mmHg)  脈拍\n\
         前日の始め  00:00  160/80  64\n山田  08:12  125/80  64\n鈴木  18:30  150/80  64\n\
         前日の終わり  23:59  160/80  64\n"
    );
    // ログは件数だけ (氏名・メールアドレスを出さない)
    assert!(logs.lock().unwrap().is_empty());
    assert!(!summary.line().contains(&d));
}

#[tokio::test]
async fn test_no_records_still_sends_a_mail_saying_so() {
    let d = domain();
    let tenant = setup(&d, &["a"]).await;
    // 前日の外の記録だけ
    let e = emp(tenant, "山田").await;
    record(
        tenant,
        e,
        "completed",
        Some(120),
        Some("2026-10-22T15:00:00Z"),
        "2026-10-22T15:00:00Z",
    )
    .await;
    let mailer = Arc::new(MockMailer::default());
    let (log, _) = logs_sink();
    let mut pg = rt_client().await;

    run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap();

    let sent = mine(&mailer, &d);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subject, "血圧の日報 (10/22)");
    assert_eq!(sent[0].body, "血圧の日報 (10/22)\n\n記録はありません\n");
}

async fn day_record(t: Uuid) {
    let e = emp(t, "山田").await;
    record(
        t,
        e,
        "completed",
        Some(160),
        Some("2026-10-22T01:00:00Z"),
        "2026-10-22T01:00:00Z",
    )
    .await;
}

async fn run_once(mailer: &Arc<MockMailer>) -> measurements::report::ReportSummary {
    let (log, _) = logs_sink();
    let mut pg = rt_client().await;
    run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap()
}

#[tokio::test]
async fn test_admins_get_the_report_by_default() {
    // 設定の行を作った管理者には、日報を受け取る既定 (daily_report = true) で届く
    let d = domain();
    let tenant = setup(&d, &["a"]).await;
    day_record(tenant).await;
    let mailer = Arc::new(MockMailer::default());

    run_once(&mailer).await;

    let sent = mine(&mailer, &d);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].to, format!("a@{d}"));
}

#[tokio::test]
async fn test_opting_out_removes_only_that_person() {
    let d = domain();
    let tenant = setup(&d, &["stays"]).await;
    recipient(
        tenant,
        Some(&format!("out@{d}")),
        false,
        true,
        &["admin"],
        false,
    )
    .await;
    day_record(tenant).await;
    let mailer = Arc::new(MockMailer::default());

    run_once(&mailer).await;

    let sent = mine(&mailer, &d);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0].to, format!("stays@{d}"));
}

#[tokio::test]
async fn test_an_admin_without_a_settings_row_gets_nothing() {
    // 設定の行が無い管理者は email が無いので送らない (既存の通知の宛先に居ても、グループに入っていても使わない)
    let d = domain();
    let tenant = tenant().await;
    let bare = recipient(tenant, None, true, false, &["admin"], false).await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipients \
                 (tenant_id, name, provider, lineworks_user_id, email, enabled, employee_id) \
             VALUES ($1, '受信者', 'lineworks', $2, $3, true, $4)",
            &[
                &tenant,
                &Uuid::new_v4().to_string(),
                &format!("bare@{d}"),
                &bare,
            ],
        )
        .await
        .unwrap();
    // 旧グループ「血圧の日報」・「血圧の日報を受け取らない」も、いまは何の意味も持たない
    for name in ["血圧の日報", "血圧の日報を受け取らない"] {
        let g: Uuid = admin()
            .await
            .query_one(
                "INSERT INTO alc_api.notify_groups (tenant_id, name) VALUES ($1, $2) RETURNING id",
                &[&tenant, &name],
            )
            .await
            .unwrap()
            .get(0);
        admin()
            .await
            .execute(
                "INSERT INTO alc_api.notify_recipient_groups (group_id, recipient_id) \
                 SELECT $1, id FROM alc_api.notify_recipients WHERE tenant_id = $2",
                &[&g, &tenant],
            )
            .await
            .unwrap();
    }
    day_record(tenant).await;
    let mailer = Arc::new(MockMailer::default());

    run_once(&mailer).await;

    assert!(mine(&mailer, &d).is_empty());
}

#[tokio::test]
async fn test_the_settings_email_is_used_not_the_notify_recipients_one() {
    // 通知の宛先の email が別に在り「受け取らない」グループに入っていても、設定の email にだけ届く
    let d = domain();
    let tenant = tenant().await;
    let adm = recipient(
        tenant,
        Some(&format!("settings@{d}")),
        true,
        true,
        &["admin"],
        false,
    )
    .await;
    let r: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.notify_recipients \
                 (tenant_id, name, provider, lineworks_user_id, email, enabled, employee_id) \
             VALUES ($1, '受信者', 'lineworks', $2, $3, true, $4) RETURNING id",
            &[
                &tenant,
                &Uuid::new_v4().to_string(),
                &format!("recipient@{d}"),
                &adm,
            ],
        )
        .await
        .unwrap()
        .get(0);
    let g: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.notify_groups (tenant_id, name) VALUES ($1, '血圧の日報を受け取らない') RETURNING id",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipient_groups (group_id, recipient_id) VALUES ($1, $2)",
            &[&g, &r],
        )
        .await
        .unwrap();
    day_record(tenant).await;
    let mailer = Arc::new(MockMailer::default());

    run_once(&mailer).await;

    let to: Vec<_> = mine(&mailer, &d).into_iter().map(|m| m.to).collect();
    assert_eq!(to, [format!("settings@{d}")]);
}

#[tokio::test]
async fn test_tenants_without_an_eligible_admin_get_nothing() {
    let d = domain();
    // 宛先が無い・管理者でない・設定の行なし・email なし・日報を受け取らない・削除済み・全員が外した。いずれも記録があっても送らない
    let none = tenant().await;
    let mut tenants = vec![none];
    for (email, daily_report, settings, role, deleted) in [
        (true, true, true, &["driver"][..], false),
        (true, true, false, &["admin"][..], false),
        (false, true, true, &["admin"][..], false),
        (true, false, true, &["admin"][..], false),
        (true, true, true, &["admin"][..], true),
    ] {
        let t = tenant().await;
        let email = email.then(|| format!("x@{d}"));
        recipient(t, email.as_deref(), daily_report, settings, role, deleted).await;
        tenants.push(t);
    }
    let all_out = tenant().await;
    recipient(
        all_out,
        Some(&format!("x@{d}")),
        false,
        true,
        &["manager"],
        false,
    )
    .await;
    tenants.push(all_out);
    for t in &tenants {
        day_record(*t).await;
    }
    let mailer = Arc::new(MockMailer::default());

    let summary = run_once(&mailer).await;

    assert!(summary.skipped >= tenants.len(), "{summary:?}");
    assert!(mine(&mailer, &d).is_empty());
}

#[tokio::test]
async fn test_another_tenants_records_are_not_mailed() {
    let d = domain();
    let a = setup(&d, &["a"]).await;
    let b = tenant().await;
    let e = emp(b, "別のテナントの人").await;
    record(
        b,
        e,
        "completed",
        Some(160),
        Some("2026-10-22T01:00:00Z"),
        "2026-10-22T01:00:00Z",
    )
    .await;
    let mailer = Arc::new(MockMailer::default());
    let (log, _) = logs_sink();
    let mut pg = rt_client().await;

    run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap();

    let sent = mine(&mailer, &d);
    assert_eq!(sent.len(), 1);
    assert!(
        sent[0].body.contains("記録はありません"),
        "{}",
        sent[0].body
    );
    assert!(!sent[0].body.contains("別のテナントの人"));
    let _ = a;
}

#[tokio::test]
async fn test_failed_sends_are_counted_and_the_rest_go_on() {
    let d = domain();
    let tenant = setup(&d, &["bad", "good"]).await;
    let e = emp(tenant, "山田").await;
    record(
        tenant,
        e,
        "completed",
        Some(120),
        Some("2026-10-22T01:00:00Z"),
        "2026-10-22T01:00:00Z",
    )
    .await;
    let mailer = MockMailer::failing(&[&format!("bad@{d}")]);
    let (log, logs) = logs_sink();
    let mut pg = rt_client().await;

    let summary = run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap();

    assert!(summary.failed >= 1 && summary.sent >= 1, "{summary:?}");
    assert_eq!(mine(&mailer, &d).len(), 1);
    assert!(mailer.attempted().contains(&format!("bad@{d}")));
    assert!(logs
        .lock()
        .unwrap()
        .contains(&"measurements: alert send failed: send".to_owned()));
}

#[tokio::test]
async fn test_without_a_mailer_nothing_is_read_or_sent() {
    let (log, logs) = logs_sink();
    let mut pg = rt_client().await;

    assert_eq!(run(&mut pg, None, &log, now()).await, None);

    assert_eq!(
        *logs.lock().unwrap(),
        ["measurements: bp report skipped: no mailer"]
    );
}

#[tokio::test]
async fn test_tenant_list_failure_is_logged_with_a_fixed_word() {
    let (log, logs) = logs_sink();
    let mut pg = broken_client().await;
    let mailer = Arc::new(MockMailer::default());

    assert_eq!(run(&mut pg, Some(mailer), &log, now()).await, None);

    let logs = logs.lock().unwrap();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].starts_with("measurements: bp report list failed: db ("));
}

#[tokio::test]
async fn test_one_tenants_lookup_failure_does_not_stop_the_others() {
    let d = domain();
    let ok = setup(&d, &["a"]).await;
    let failing = setup(&d, &["b"]).await;
    let _ = ok;
    // この tenant の宛先の行を読むと必ず失敗する制限的な policy
    let su = admin().await;
    let policy = format!("report_fail_{}", failing.simple());
    su.batch_execute(&format!(
        "CREATE POLICY {policy} ON alc_api.bp_notify_settings AS RESTRICTIVE FOR SELECT \
         USING (CASE WHEN tenant_id = '{failing}'::uuid THEN 1 / (length(email) - length(email)) = 1 ELSE true END)"
    ))
    .await
    .unwrap();
    let mailer = Arc::new(MockMailer::default());
    let (log, logs) = logs_sink();
    let mut pg = rt_client().await;

    let summary = run(&mut pg, Some(mailer.clone()), &log, now())
        .await
        .unwrap();

    su.batch_execute(&format!(
        "DROP POLICY {policy} ON alc_api.bp_notify_settings"
    ))
    .await
    .unwrap();
    assert!(summary.lookup_failed >= 1, "{summary:?}");
    // 失敗したテナントの宛先 (b) には届かず、ほかのテナント (a) には届く
    assert_eq!(
        mailer
            .attempted()
            .iter()
            .filter(|t| **t == format!("b@{d}"))
            .count(),
        0
    );
    assert_eq!(mine(&mailer, &d).len(), 1);
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|l| l.starts_with("measurements: bp report lookup failed: db (")));
}
