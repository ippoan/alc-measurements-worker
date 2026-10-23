//! 血圧の本人への LINE WORKS (Refs ippoan/alc-app#436) を口から実 DB まで通す。送り先の Worker (alc-lineworks-worker の
//! Service Binding) は `MockService` (受けた要求を記録する)。
//!
//! 条件は `PUT /measurements/{id}` が **status = completed で systolic が入り、更新前の systolic が NULL** の保存だけ。
//! 宛先は血圧の通知の設定の表 `bp_notify_settings` (alc-migrations 186) の、測定の乗務員の `notify_self` かつ
//! `lineworks_user_id` ありの行 (既存の `notify_recipients` は使わない)。dev 端末の要求からも送る (本人宛て)。ログは固定の語と件数だけ (氏名・値・id・user_id を出さない)。

mod support;

use std::sync::Arc;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const SKIP_NO_RECIPIENT: &str = "measurements: bp notify skipped: no recipient";
const SKIP_NO_LINEWORKS: &str = "measurements: bp notify skipped: no lineworks";
/// 2026-10-23 08:12 (日本時間)
const AT: &str = "2026-10-22T23:12:00Z";

fn app_with(service: &Arc<MockService>) -> impl std::future::Future<Output = App> {
    let service: measurements::service::SharedService = service.clone();
    async move { build_lineworks(rt_client().await, Some(service)) }
}

/// テナントと乗務員
async fn setup() -> (Uuid, Uuid) {
    let tenant = tenant().await;
    let emp = employee(
        tenant,
        "山田太郎",
        &Uuid::new_v4().simple().to_string()[..8],
    )
    .await;
    (tenant, emp)
}

/// 血圧の通知の設定の行を 1 つ作る (superuser で。alc-migrations 186)。`lineworks_user_id` が無ければ LINE WORKS の宛先なし
async fn recipient(
    tenant: Uuid,
    employee: Uuid,
    lineworks_user_id: Option<&str>,
    notify_self: bool,
) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bp_notify_settings (tenant_id, employee_id, lineworks_user_id, notify_self) \
             VALUES ($1, $2, $3, $4)",
            &[&tenant, &employee, &lineworks_user_id, &notify_self],
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

/// 血圧つきの完了 PUT の本文
fn bp_body(systolic: i32, diastolic: i32, pulse: Option<i32>) -> Value {
    let mut body = json!({
        "status": "completed",
        "systolic": systolic,
        "diastolic": diastolic,
        "medical_measured_at": AT,
    });
    if let Some(p) = pulse {
        body["pulse"] = json!(p);
    }
    body
}

fn texts(service: &MockService) -> Vec<String> {
    service
        .requests()
        .iter()
        .map(|r| {
            let v: Value = serde_json::from_str(&r.body).unwrap();
            v["content"]["text"].as_str().unwrap().to_owned()
        })
        .collect()
}

#[tokio::test]
async fn test_first_save_sends_the_result_to_the_person() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;
    let id = start_measurement(&app, tenant, emp).await;

    put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, Some(66))).await;

    let requests = service.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/api/internal/lineworks/messages");
    assert!(requests[0]
        .headers
        .contains(&("X-Tenant-ID", tenant.to_string())));
    let sent: Value = serde_json::from_str(&requests[0].body).unwrap();
    assert_eq!(
        sent,
        json!({
            "to": {"user_id": "lw-user-1"},
            "content": {
                "type": "text",
                "text": "血圧の記録 (10/23 08:12)\n最高 124 / 最低 78 mmHg\n脈拍 66"
            }
        })
    );
    // 成功のログは残さない
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_saving_again_does_not_send_twice() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;
    let id = start_measurement(&app, tenant, emp).await;

    put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, Some(66))).await;
    // 同じ値の保存し直し・別の値での保存し直し・血圧を送らない保存 (動画の URL など) のどれも送らない
    put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, Some(66))).await;
    put_measurement_ok(&app, tenant, &id, &bp_body(150, 95, Some(80))).await;
    put_measurement_ok(
        &app,
        tenant,
        &id,
        &json!({"video_url": "https://files.example.test/v.webm"}),
    )
    .await;

    assert_eq!(service.requests().len(), 1);
}

#[tokio::test]
async fn test_not_completed_or_without_systolic_does_not_send() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;
    let id = start_measurement(&app, tenant, emp).await;

    // 血圧が無い完了、status が started のままの保存
    put_measurement_ok(&app, tenant, &id, &json!({"status": "completed"})).await;
    let id2 = start_measurement(&app, tenant, emp).await;
    put_measurement_ok(
        &app,
        tenant,
        &id2,
        &json!({"systolic": 120, "diastolic": 80}),
    )
    .await;

    assert!(service.requests().is_empty());
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_off_baseline_adds_a_line_with_the_default_baseline() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;

    // 既定 120/80 ±10: 最高 131 は外れ、最低が外れても (69) 外れ。端 (130/70) は外れではない
    for (sys, dia, off) in [(131, 80, true), (120, 69, true), (130, 70, false)] {
        let id = start_measurement(&app, tenant, emp).await;
        put_measurement_ok(&app, tenant, &id, &bp_body(sys, dia, None)).await;
        let text = texts(&service).pop().unwrap();
        assert_eq!(
            text.ends_with("\n基準から外れています (基準 120/80)"),
            off,
            "{text}"
        );
        // 脈拍が無ければその行は無い
        assert!(!text.contains("脈拍"), "{text}");
    }
}

#[tokio::test]
async fn test_off_baseline_uses_the_employees_own_baseline() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    baseline(tenant, emp, 150, 95, 5).await;
    let service = MockService::ok();
    let app = app_with(&service).await;

    // 既定なら外れる 140/90 も、この人の基準 150/95 ±5 では最高が外れ (差 10)、145/92 は外れない
    let id = start_measurement(&app, tenant, emp).await;
    put_measurement_ok(&app, tenant, &id, &bp_body(140, 90, Some(70))).await;
    let id = start_measurement(&app, tenant, emp).await;
    put_measurement_ok(&app, tenant, &id, &bp_body(145, 92, Some(70))).await;

    let texts = texts(&service);
    assert!(
        texts[0].ends_with("\n脈拍 70\n基準から外れています (基準 150/95)"),
        "{}",
        texts[0]
    );
    assert!(!texts[1].contains("基準から外れています"), "{}", texts[1]);
}

#[tokio::test]
async fn test_only_the_matching_enabled_recipient_receives() {
    let (tenant, emp) = setup().await;
    let code = || Uuid::new_v4().simple().to_string()[..8].to_owned();
    let other_emp = employee(tenant, "鈴木", &code()).await;
    let line_only_emp = employee(tenant, "佐藤", &code()).await;
    let bare_emp = employee(tenant, "高橋", &code()).await;
    let (other_tenant, other_tenant_emp) = setup().await;
    let service = MockService::ok();
    let app = app_with(&service).await;

    // 送らない: 本人への通知を切った・user_id が無い・設定の行が無い。本人でない設定: 別の社員・別テナント。
    // 既存の通知の宛先 (notify_recipients。結び付けも user_id もある) は使わない
    recipient(tenant, emp, Some("lw-disabled"), false).await;
    recipient(tenant, line_only_emp, None, true).await;
    recipient(tenant, other_emp, Some("lw-other-employee"), true).await;
    recipient(
        other_tenant,
        other_tenant_emp,
        Some("lw-other-tenant"),
        true,
    )
    .await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.notify_recipients \
                 (tenant_id, name, provider, lineworks_user_id, enabled, employee_id) \
             VALUES ($1, '受信者', 'lineworks', 'lw-old-recipient', true, $2), \
                    ($1, '受信者', 'lineworks', 'lw-old-recipient-2', true, $3)",
            &[&tenant, &emp, &bare_emp],
        )
        .await
        .unwrap();
    for who in [emp, line_only_emp, bare_emp] {
        let id = start_measurement(&app, tenant, who).await;
        put_measurement_ok(&app, tenant, &id, &bp_body(120, 80, None)).await;
    }
    assert!(service.requests().is_empty());
    assert_eq!(
        app.logs(),
        [SKIP_NO_RECIPIENT, SKIP_NO_RECIPIENT, SKIP_NO_RECIPIENT]
    );

    // 本人への通知を入れ直したら、その 1 人だけ
    admin()
        .await
        .execute(
            "UPDATE alc_api.bp_notify_settings SET notify_self = true WHERE lineworks_user_id = 'lw-disabled'",
            &[],
        )
        .await
        .unwrap();
    let id = start_measurement(&app, tenant, emp).await;
    put_measurement_ok(&app, tenant, &id, &bp_body(120, 80, None)).await;
    let sent: Vec<Value> = service
        .requests()
        .iter()
        .map(|r| serde_json::from_str(&r.body).unwrap())
        .collect();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["to"]["user_id"], "lw-disabled");
}

#[tokio::test]
async fn test_dev_device_sends_to_the_person_too() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;
    let dev = [("X-Device-Dev", "1"), ("content-type", "application/json")];
    let start = call_raw(
        &app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        &dev,
        serde_json::to_vec(&json!({ "employee_id": emp })).unwrap(),
    )
    .await;
    assert_eq!(start.status, 201);
    let id = start.json()["id"].as_str().unwrap().to_owned();

    let res = call_raw(
        &app,
        "PUT",
        &format!("/api/measurements/{id}"),
        Some(tenant),
        &dev,
        serde_json::to_vec(&bp_body(124, 78, Some(66))).unwrap(),
    )
    .await;

    assert_eq!(res.status, 200);
    let requests = service.requests();
    assert_eq!(requests.len(), 1);
    let sent: Value = serde_json::from_str(&requests[0].body).unwrap();
    assert_eq!(sent["to"]["user_id"], "lw-user-1");
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_no_lineworks_binding_keeps_the_save() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let app = build_lineworks(rt_client().await, None);
    let id = start_measurement(&app, tenant, emp).await;

    let res = put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, None)).await;

    assert_eq!(res["systolic"], 124);
    assert_eq!(app.logs(), [SKIP_NO_LINEWORKS]);
}

#[tokio::test]
async fn test_send_failures_keep_the_save_and_log_only_a_label() {
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-secret-user"), true).await;
    for (service, want) in [
        (
            MockService::with(404, false),
            "measurements: bp notify failed: lineworks (status 404)",
        ),
        (
            MockService::with(204, true),
            "measurements: bp notify failed: lineworks (fetch)",
        ),
    ] {
        let app = app_with(&service).await;
        let id = start_measurement(&app, tenant, emp).await;

        let res = put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, Some(66))).await;

        // 保存は成功のまま。ログは固定の語だけ (user_id・値・氏名・id を出さない)
        assert_eq!(res["systolic"], 124);
        assert_eq!(app.logs(), [want]);
        assert_eq!(service.requests().len(), 1);
    }
}

#[tokio::test]
async fn test_lookup_failure_keeps_the_save_and_logs_a_fixed_word() {
    // 宛先の引きが失敗しても (この tenant の宛先の行を読むと必ず失敗する制限的な policy を張る)、保存は残り、送らない
    let (tenant, emp) = setup().await;
    recipient(tenant, emp, Some("lw-user-1"), true).await;
    let service = MockService::ok();
    let app = app_with(&service).await;
    let id = start_measurement(&app, tenant, emp).await;
    let su = admin().await;
    let policy = format!("bp_fail_{}", tenant.simple());
    su.batch_execute(&format!(
        "CREATE POLICY {policy} ON alc_api.bp_notify_settings AS RESTRICTIVE FOR SELECT \
         USING (CASE WHEN tenant_id = '{tenant}'::uuid THEN 1 / (length(tenant_id::text) - length(tenant_id::text)) = 1 ELSE true END)"
    ))
    .await
    .unwrap();

    let res = put_measurement_ok(&app, tenant, &id, &bp_body(124, 78, None)).await;

    su.batch_execute(&format!(
        "DROP POLICY {policy} ON alc_api.bp_notify_settings"
    ))
    .await
    .unwrap();
    assert_eq!(res["systolic"], 124);
    assert!(service.requests().is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: bp notify lookup failed: db ("),
        "{logs:?}"
    );
}
