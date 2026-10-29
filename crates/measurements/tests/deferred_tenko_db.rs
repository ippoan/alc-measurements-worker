//! 点呼の記録の確定を後回しにする保存 (`PUT` の `defer_tenko`)・確定の口 (`POST /api/measurements/{id}/tenko`)・
//! 見回り (`deferred::sweep`) を実 DB で固定する (Refs ippoan/alc-app#436)。rust には無い振る舞い。
//!
//! 見回りの一覧は全テナントをまたぐ (関数 `list_pending_tenko`、alc-migrations 180)。並列に走るテストの行を拾わないよう、
//! 確定待ちの行の時刻を戻す (2 分より前にする) のは見回りのテスト 1 本だけにする。

mod support;

use measurements::deferred::{sweep, SweepSummary};
use measurements::LogSink;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex as StdMutex};
use support::*;
use uuid::Uuid;

const DEV: (&str, &str) = ("X-Device-Dev", "1");
const JSON: (&str, &str) = ("content-type", "application/json");

/// 完了 PUT の本文 (`defer_tenko: true`、始業・本人確認・車検証の番号付き)
fn deferred_body(result_type: &str, measured_at: &str) -> Value {
    let mut body = completed_body(result_type, measured_at, true, Some("pre_operation"), None);
    body["defer_tenko"] = json!(true);
    body["identity_method"] = json!("license");
    body["carins_cert_no"] = json!("123456789012");
    body
}

/// `dev` なら dev端末の印を付けて打つ
async fn call_as(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Uuid,
    dev: bool,
    body: Option<Value>,
) -> Got {
    let mut headers = vec![JSON];
    if dev {
        headers.push(DEV);
    }
    let bytes = body
        .map(|b| serde_json::to_vec(&b).unwrap())
        .unwrap_or_default();
    call_raw(app, method, uri, Some(tenant), &headers, bytes).await
}

/// 測定を開始し、確定を後回しにして完了する (応答の JSON と id)
async fn deferred_measurement(
    app: &App,
    tenant: Uuid,
    emp: Uuid,
    result_type: &str,
    measured_at: &str,
    dev: bool,
) -> (String, Value) {
    let res = call_as(
        app,
        "POST",
        "/api/measurements/start",
        tenant,
        dev,
        Some(json!({ "employee_id": emp })),
    )
    .await;
    assert_eq!(res.status, 201);
    let id = res.json()["id"].as_str().unwrap().to_string();
    let res = call_as(
        app,
        "PUT",
        &format!("/api/measurements/{id}"),
        tenant,
        dev,
        Some(deferred_body(result_type, measured_at)),
    )
    .await;
    assert_eq!(res.status, 200);
    (id, res.json())
}

async fn confirm_as(app: &App, tenant: Uuid, id: &str, dev: bool, body: Value) -> Got {
    call_as(
        app,
        "POST",
        &format!("/api/measurements/{id}/tenko"),
        tenant,
        dev,
        Some(body),
    )
    .await
}

async fn confirm(app: &App, tenant: Uuid, id: &str, method: &str) -> Got {
    confirm_as(app, tenant, id, false, json!({ "tenko_method": method })).await
}

/// 測定の `tenko_pending` (superuser で直接)
async fn pending_of(id: &str) -> Option<Value> {
    admin()
        .await
        .query_one(
            "SELECT tenko_pending FROM alc_api.measurements WHERE id = $1",
            &[&Uuid::parse_str(id).unwrap()],
        )
        .await
        .unwrap()
        .get(0)
}

/// 確定待ちの測定の完了の時刻を 3 分前にする (見回りの一覧に載る)
async fn age(id: &str) {
    admin()
        .await
        .execute(
            "UPDATE alc_api.measurements SET updated_at = now() - interval '3 minutes' WHERE id = $1",
            &[&Uuid::parse_str(id).unwrap()],
        )
        .await
        .unwrap();
}

/// 測定に紐づく session と record の is_dev
async fn dev_flags(tenant: Uuid, id: &str) -> Vec<(bool, bool)> {
    admin()
        .await
        .query(
            "SELECT s.is_dev, r.is_dev FROM alc_api.tenko_sessions s
             JOIN alc_api.tenko_records r ON r.session_id = s.id
             WHERE s.tenant_id = $1 AND s.measurement_id = $2",
            &[&tenant, &Uuid::parse_str(id).unwrap()],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn log_sink() -> (LogSink, Arc<StdMutex<Vec<String>>>) {
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    (
        Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        logs,
    )
}

#[tokio::test]
async fn test_deferred_put_keeps_input_and_creates_no_record() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF1").await;

    let (id, res) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T01:00:00Z", false).await;
    assert!(res["tenko_session_id"].is_null());
    assert_eq!(res["tenko_pending"], true);
    assert_eq!(res["status"], "completed", "測定は保存されている");
    assert_eq!(
        counts_any_method(tenant).await,
        (0, 0),
        "記録はまだ作らない"
    );
    // 入力は点呼方法を除いた 4 つ
    assert_eq!(
        pending_of(&id).await,
        Some(json!({
            "tenko_type": "pre_operation",
            "identity_method": "license",
            "carins_cert_no": "123456789012",
            "carins_vehicle_id": null,
        }))
    );
    // 取得の応答に列は出ない (測定の形は rust と同じ)
    let got = call(
        &app,
        "GET",
        &format!("/api/measurements/{id}"),
        Some(tenant),
        None,
    )
    .await;
    assert!(got.json().get("tenko_pending").is_none());
}

#[tokio::test]
async fn test_put_without_defer_is_unchanged() {
    // defer_tenko が無い・false なら今までどおり記録まで作り、応答に tenko_pending は出ない
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF2").await;
    for (n, defer) in [(0, None), (1, Some(false))] {
        let id = start_measurement(&app, tenant, emp).await;
        let mut body = completed_body(
            "normal",
            &format!("2026-10-20T0{n}:10:00Z"),
            true,
            None,
            None,
        );
        if let Some(d) = defer {
            body["defer_tenko"] = json!(d);
        }
        let res = put_measurement_ok(&app, tenant, &id, &body).await;
        assert!(res["tenko_session_id"].is_string(), "{defer:?}");
        assert!(res.get("tenko_pending").is_none(), "{defer:?}");
        assert_eq!(pending_of(&id).await, None);
    }
    assert_eq!(counts(tenant).await, (2, 2));

    // record_as_tenko が無ければ defer_tenko だけでは確定待ちにしない
    let id = start_measurement(&app, tenant, emp).await;
    let mut body = completed_body("normal", "2026-10-20T03:10:00Z", false, None, None);
    body["defer_tenko"] = json!(true);
    let res = put_measurement_ok(&app, tenant, &id, &body).await;
    assert!(res["tenko_session_id"].is_null());
    assert!(res.get("tenko_pending").is_none());
    assert_eq!(pending_of(&id).await, None);
}

#[tokio::test]
async fn test_later_put_does_not_overwrite_pending_or_record() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF3").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T02:00:00Z", false).await;
    let before = pending_of(&id).await;

    // 録画の URL の保存 (record_as_tenko 付き・defer 付き・別の入力) でも入力は変わらず、記録も作らない
    for body in [
        json!({ "video_url": "https://example.invalid/v.webm", "record_as_tenko": true }),
        json!({ "status": "completed", "record_as_tenko": true, "defer_tenko": true, "tenko_type": "post_operation" }),
    ] {
        let res = put_measurement_ok(&app, tenant, &id, &body).await;
        assert!(res["tenko_session_id"].is_null(), "{body}");
        assert_eq!(res["tenko_pending"], true, "{body}");
    }
    assert_eq!(pending_of(&id).await, before);
    assert_eq!(counts_any_method(tenant).await, (0, 0));

    // 確定した後の PUT は確定待ちに戻さない (record_as_tenko なら確定済みの session の id を返す)
    let sid = confirm(&app, tenant, &id, "通常点呼").await.json()["tenko_session_id"].clone();
    let res = put_measurement_ok(
        &app,
        tenant,
        &id,
        &deferred_body("normal", "2026-10-20T02:00:00Z"),
    )
    .await;
    assert_eq!(res["tenko_session_id"], sid);
    assert!(res.get("tenko_pending").is_none());
    assert_eq!(pending_of(&id).await, None);
    assert_eq!(counts_any_method(tenant).await, (1, 1));
}

#[tokio::test]
async fn test_confirm_normal_tenko_records_once() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF4").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T03:00:00Z", false).await;

    let res = confirm(&app, tenant, &id, "通常点呼").await;
    assert_eq!(res.status, 200);
    let body = res.json();
    assert_eq!(body["tenko_method"], "通常点呼");
    let sid = body["tenko_session_id"].as_str().unwrap().to_string();
    assert_eq!(pending_of(&id).await, None, "確定したら入力は消える");
    // 入力 (種別・本人確認・車検証の番号) が記録に入る
    let (sessions, records) = type_and_method(tenant, &id).await;
    assert_eq!(sessions, vec![("pre_operation".into(), "通常点呼".into())]);
    assert_eq!(records, vec![("pre_operation".into(), "通常点呼".into())]);
    let (identity, _) = identity_methods(tenant, &id).await;
    assert_eq!(identity, vec![Some("license".to_string())]);
    let cert: Option<String> = admin()
        .await
        .query_one(
            "SELECT carins_cert_no FROM alc_api.tenko_sessions WHERE id = $1",
            &[&Uuid::parse_str(&sid).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(cert.as_deref(), Some("123456789012"));

    // 2 回目 (同じ方法) は同じ id で、記録は増えない
    let again = confirm(&app, tenant, &id, "通常点呼").await;
    assert_eq!(again.status, 200);
    assert_eq!(again.json()["tenko_session_id"], sid.as_str());
    assert_eq!(counts(tenant).await, (1, 1));

    // 方法が違えば 409 で、既にある方法を返す
    let other = confirm(&app, tenant, &id, "IT点呼").await;
    assert_eq!(other.status, 409);
    assert_eq!(
        other.json(),
        json!({ "error": "tenko_method_conflict", "tenko_method": "通常点呼" })
    );
    assert_eq!(counts_any_method(tenant).await, (1, 1));
}

#[tokio::test]
async fn test_confirm_it_tenko_records_method() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF5").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T04:00:00Z", false).await;

    let res = confirm(&app, tenant, &id, "IT点呼").await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["tenko_method"], "IT点呼");
    let (sessions, records) = type_and_method(tenant, &id).await;
    assert_eq!(sessions, vec![("pre_operation".into(), "IT点呼".into())]);
    // IT点呼 は判定待ちで作り、記録は運行管理者の判定で書く
    assert!(records.is_empty());

    let again = confirm(&app, tenant, &id, "IT点呼").await;
    assert_eq!(
        again.json()["tenko_session_id"],
        res.json()["tenko_session_id"]
    );
    let other = confirm(&app, tenant, &id, "通常点呼").await;
    assert_eq!(other.status, 409);
    assert_eq!(other.json()["tenko_method"], "IT点呼");
}

#[tokio::test]
async fn test_confirm_rejects() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF6").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T05:00:00Z", false).await;

    // 方法が 2 値以外・無い → 400 (確定待ちのまま)
    for body in [
        json!({ "tenko_method": "自動点呼" }),
        json!({ "tenko_method": "" }),
        json!({}),
    ] {
        let res = confirm_as(&app, tenant, &id, false, body.clone()).await;
        assert_eq!(res.status, 400, "{body}");
    }
    assert!(pending_of(&id).await.is_some());

    // 無い測定・別テナント・dev の軸の違い → 404
    let other = tenant_with_employee().await;
    for (t, mid, dev) in [
        (tenant, Uuid::new_v4().to_string(), false),
        (other, id.clone(), false),
        (tenant, id.clone(), true),
    ] {
        let res = confirm_as(&app, t, &mid, dev, json!({ "tenko_method": "通常点呼" })).await;
        assert_eq!(res.status, 404, "{t} {mid} {dev}");
    }
    assert!(pending_of(&id).await.is_some(), "404 では確定しない");

    // 未完了 → 409
    let started = start_measurement(&app, tenant, emp).await;
    let res = confirm(&app, tenant, &started, "通常点呼").await;
    assert_eq!(res.status, 409);
    assert_eq!(
        res.json(),
        json!({ "error": "measurement_not_completed", "tenko_method": null })
    );

    // 確定を後回しにしていない完了済みの測定 (記録なし) → 409
    let plain = start_measurement(&app, tenant, emp).await;
    put_measurement_ok(
        &app,
        tenant,
        &plain,
        &completed_body("normal", "2026-10-20T05:10:00Z", false, None, None),
    )
    .await;
    let res = confirm(&app, tenant, &plain, "通常点呼").await;
    assert_eq!(res.status, 409);
    assert_eq!(
        res.json(),
        json!({ "error": "no_pending_tenko", "tenko_method": null })
    );

    // 点呼にならない結果 (error) は確定しても記録ができず 409 で、入力は消える (見回りが拾い続けない)
    let (err_id, _) =
        deferred_measurement(&app, tenant, emp, "error", "2026-10-20T05:20:00Z", false).await;
    assert!(pending_of(&err_id).await.is_some());
    let res = confirm(&app, tenant, &err_id, "通常点呼").await;
    assert_eq!(res.status, 409);
    assert_eq!(res.json()["error"], "no_pending_tenko");
    assert_eq!(pending_of(&err_id).await, None);
    assert_eq!(counts_any_method(tenant).await, (0, 0));
}

async fn tenant_with_employee() -> Uuid {
    let t = tenant().await;
    employee(t, "別テナント", "DFX").await;
    t
}

#[tokio::test]
async fn test_confirm_dev_measurement_records_dev_rows() {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF7").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T06:00:00Z", true).await;
    let res = confirm_as(
        &app,
        tenant,
        &id,
        true,
        json!({ "tenko_method": "通常点呼" }),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(dev_flags(tenant, &id).await, vec![(true, true)]);
}

#[tokio::test]
async fn test_confirm_keeps_pending_when_record_fails() {
    // 記録の INSERT が失敗したら transaction ごと戻し (500)、入力は残す (見回りがまた拾う)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF8").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T07:00:00Z", false).await;
    fail_record_insert(tenant).await;
    let res = confirm(&app, tenant, &id, "通常点呼").await;
    drop_fail_record_insert(tenant).await;
    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    assert!(pending_of(&id).await.is_some());
    assert_eq!(counts_any_method(tenant).await, (0, 0));
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: tenko confirm failed: db ("),
        "{}",
        logs[0]
    );
    assert!(!logs[0].contains(&id) && !logs[0].contains(&tenant.to_string()));

    // 直った後は確定できる
    assert_eq!(confirm(&app, tenant, &id, "通常点呼").await.status, 200);
}

#[tokio::test]
async fn test_confirm_unreadable_pending_records_with_defaults() {
    // 入力が読めない (手で書き換えた) ときは既定 (種別 normal) で記録する
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DF9").await;
    let (id, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T08:00:00Z", false).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.measurements SET tenko_pending = '\"garbage\"'::jsonb WHERE id = $1",
            &[&Uuid::parse_str(&id).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(confirm(&app, tenant, &id, "通常点呼").await.status, 200);
    let (sessions, _) = type_and_method(tenant, &id).await;
    assert_eq!(sessions, vec![("normal".into(), "通常点呼".into())]);
}

#[tokio::test]
async fn test_confirm_db_failure_is_500() {
    let app = build(broken_client().await);
    let tenant = tenant().await;
    let id = Uuid::new_v4();
    let res = confirm(&app, tenant, &id.to_string(), "通常点呼").await;
    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].starts_with("measurements: tenko confirm failed: db ("));
}

#[tokio::test]
async fn test_sweep_list_failure_logs_fixed_words() {
    let mut pg = broken_client().await;
    let (log, logs) = log_sink();
    assert_eq!(sweep(&mut pg, &log).await, None);
    let logs = logs.lock().unwrap().clone();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].starts_with("measurements: tenko sweep list failed: db ("));
}

#[test]
fn test_sweep_summary_line_has_counts_only() {
    let s = SweepSummary {
        listed: 4,
        confirmed: 2,
        skipped: 1,
        failed: 1,
    };
    assert_eq!(
        s.line(),
        "measurements: tenko sweep: listed 4 confirmed 2 skipped 1 failed 1"
    );
}

#[tokio::test]
async fn test_sweep_confirms_only_aged_pending_as_normal_tenko() {
    // 見回りの一覧は全テナントをまたぐので、時刻を戻すのはこのテストだけ (ほかのテストの確定待ちは 2 分以内で載らない)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DS1").await;
    let failing = tenant_with_employee().await;
    let failing_emp = employee(failing, "運行者", "DS2").await;

    let (aged, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T09:00:00Z", false).await;
    let (fresh, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T09:10:00Z", false).await;
    let (aged_dev, _) =
        deferred_measurement(&app, tenant, emp, "over", "2026-10-20T09:20:00Z", true).await;
    let (aged_error, _) =
        deferred_measurement(&app, tenant, emp, "error", "2026-10-20T09:30:00Z", false).await;
    // 先に端末が IT点呼 で確定した測定 (入力はもう無い = 一覧に載らない)
    let (confirmed, _) =
        deferred_measurement(&app, tenant, emp, "normal", "2026-10-20T09:40:00Z", false).await;
    assert_eq!(
        confirm(&app, tenant, &confirmed, "IT点呼").await.status,
        200
    );
    // 記録に失敗するテナントの測定
    let (aged_failing, _) = deferred_measurement(
        &app,
        failing,
        failing_emp,
        "normal",
        "2026-10-20T09:50:00Z",
        false,
    )
    .await;
    for id in [&aged, &aged_dev, &aged_error, &confirmed, &aged_failing] {
        age(id).await;
    }
    fail_record_insert(failing).await;

    let mut pg = rt_client().await;
    let (log, logs) = log_sink();
    let summary = sweep(&mut pg, &log).await.unwrap();
    drop_fail_record_insert(failing).await;
    // 載るのは時刻を戻した確定待ちの 4 件 (このテストの外の行は載らない)
    assert_eq!(
        summary,
        SweepSummary {
            listed: 4,
            confirmed: 2,
            skipped: 1,
            failed: 1,
        }
    );
    let logs = logs.lock().unwrap().clone();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].starts_with("measurements: tenko sweep confirm failed: db ("));
    assert!(!logs[0].contains(&failing.to_string()) && !logs[0].contains(&aged_failing));

    // 2 分を過ぎたものは 通常点呼 で確定 (dev の測定は dev の記録・中止の結果は中止の記録)
    let (sessions, records) = type_and_method(tenant, &aged).await;
    assert_eq!(sessions, vec![("pre_operation".into(), "通常点呼".into())]);
    assert_eq!(records, vec![("pre_operation".into(), "通常点呼".into())]);
    assert_eq!(dev_flags(tenant, &aged_dev).await, vec![(true, true)]);
    assert_eq!(
        sessions_for_measurement(tenant, &aged_dev).await,
        vec![(
            "pre_operation".into(),
            "cancelled".into(),
            Some("アルコール検知".into())
        )]
    );
    // 2 分以内は確定待ちのまま
    assert!(pending_of(&fresh).await.is_some());
    assert!(session_rows(tenant, &fresh).await.is_empty());
    // 点呼にならない結果は入力だけ消える
    assert_eq!(pending_of(&aged_error).await, None);
    assert!(session_rows(tenant, &aged_error).await.is_empty());
    // 端末が先に確定したものは触らない
    let (sessions, _) = type_and_method(tenant, &confirmed).await;
    assert_eq!(sessions, vec![("pre_operation".into(), "IT点呼".into())]);
    // 失敗した測定は入力が残り、次の回に確定する
    assert!(pending_of(&aged_failing).await.is_some());
    let summary = sweep(&mut pg, &log).await.unwrap();
    assert_eq!(
        summary,
        SweepSummary {
            listed: 1,
            confirmed: 1,
            skipped: 0,
            failed: 0,
        }
    );
    let (sessions, _) = type_and_method(failing, &aged_failing).await;
    assert_eq!(sessions, vec![("pre_operation".into(), "通常点呼".into())]);
}

/// 点呼記録の INSERT を**この tenant の行に限って**失敗させる trigger を張る (normal_tenko_db.rs と同じ形。
/// この binary では 2 本のテストが並んで張るので、関数も tenant ごとの名前にする)
async fn fail_record_insert(tenant: Uuid) {
    let t = tenant.simple();
    admin()
        .await
        .batch_execute(&format!(
            "CREATE FUNCTION alc_api.fail_deferred_record_{t}() RETURNS TRIGGER AS $$
             BEGIN
                 RAISE EXCEPTION 'injected failure for test';
             END;
             $$ LANGUAGE plpgsql;
             CREATE TRIGGER trg_fail_deferred_record_{t}
             BEFORE INSERT ON alc_api.tenko_records
             FOR EACH ROW WHEN (NEW.tenant_id = '{tenant}')
             EXECUTE FUNCTION alc_api.fail_deferred_record_{t}()"
        ))
        .await
        .unwrap();
}

async fn drop_fail_record_insert(tenant: Uuid) {
    let t = tenant.simple();
    admin()
        .await
        .batch_execute(&format!(
            "DROP TRIGGER trg_fail_deferred_record_{t} ON alc_api.tenko_records;
             DROP FUNCTION alc_api.fail_deferred_record_{t}()"
        ))
        .await
        .unwrap();
}
