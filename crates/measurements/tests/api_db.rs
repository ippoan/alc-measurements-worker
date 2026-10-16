//! 測定の 5 口を口から実 DB まで通す (rust-alc-api の tests/measurements_test.rs と、
//! tests/mock_tests/mock_measurements_test.rs のうち口の振る舞いを見るものの写し)。
//!
//! 口ごとに: 検証の 400 の各定数・404・201 / 200 の応答 JSON の形・page / per_page の境界・不正 JSON と不正 UUID の status。
//! 点呼の記録 (副作用) は tests/normal_tenko_db.rs。

mod support;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

/// rust の `Measurement` の JSON の key (応答の形。rust の mock テストの一覧と同じ)
const MEASUREMENT_KEYS: [&str; 19] = [
    "alcohol_value",
    "created_at",
    "device_use_count",
    "diastolic",
    "employee_id",
    "face_photo_url",
    "face_verified",
    "id",
    "measured_at",
    "medical_manual_input",
    "medical_measured_at",
    "pulse",
    "result_type",
    "status",
    "systolic",
    "temperature",
    "tenant_id",
    "updated_at",
    "video_url",
];

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

fn measurement_keys() -> Vec<String> {
    MEASUREMENT_KEYS.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn test_measurement_tenant_isolation() {
    // テナント分離 (rust の measurements_test.rs)
    let app = app().await;
    let tenant_a = tenant().await;
    let tenant_b = tenant().await;
    let emp = employee(tenant_a, "EmpA", "EA1").await;
    let m = post_measurement(&app, tenant_a, emp, "pass", "2026-09-12T01:00:00Z", false).await;
    let m_id = m["id"].as_str().unwrap();

    let a = call(&app, "GET", "/api/measurements", Some(tenant_a), None).await;
    assert_eq!(a.json()["total"], 1);
    let b = call(&app, "GET", "/api/measurements", Some(tenant_b), None).await;
    assert_eq!(b.json()["total"], 0);
    let res = call(
        &app,
        "GET",
        &format!("/api/measurements/{m_id}"),
        Some(tenant_b),
        None,
    )
    .await;
    assert_eq!(res.status, 404);
    // 別の tenant からの PUT も 404 で、行は変わらない
    let res = call(
        &app,
        "PUT",
        &format!("/api/measurements/{m_id}"),
        Some(tenant_b),
        Some(json!({ "status": "started" })),
    )
    .await;
    assert_eq!(res.status, 404);
    assert_eq!(measurement_status(m_id).await, "completed");
}

#[tokio::test]
async fn test_all_routes_need_tenant_header() {
    // X-Tenant-ID が無い・UUID でない → 401 (rust の test_measurements_no_auth。顔写真・動画の口も同じ)
    let app = app().await;
    let id = Uuid::new_v4();
    let routes = [
        ("GET", "/api/measurements".to_string(), None),
        (
            "POST",
            "/api/measurements".to_string(),
            Some(json!({"employee_id": id, "alcohol_value": 0.0, "result_type": "pass"})),
        ),
        (
            "POST",
            "/api/measurements/start".to_string(),
            Some(json!({"employee_id": id})),
        ),
        ("GET", format!("/api/measurements/{id}"), None),
        ("GET", format!("/api/measurements/{id}/face-photo"), None),
        ("GET", format!("/api/measurements/{id}/video"), None),
        (
            "PUT",
            format!("/api/measurements/{id}"),
            Some(json!({"status": "completed"})),
        ),
    ];
    for (method, uri, body) in routes {
        let res = call(&app, method, &uri, None, body.clone()).await;
        assert_eq!(res.status, 401, "{method} {uri}");
        let bytes = body
            .map(|b| serde_json::to_vec(&b).unwrap())
            .unwrap_or_default();
        let res = call_raw(
            &app,
            method,
            &uri,
            None,
            &[
                ("X-Tenant-ID", "not-a-uuid"),
                ("content-type", "application/json"),
            ],
            bytes,
        )
        .await;
        assert_eq!(res.status, 401, "{method} {uri} (UUID でない)");
    }
}

#[tokio::test]
async fn test_start_measurement() {
    // POST /measurements/start — 201 と応答の形 (status = started・結果は未だ無い)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "ST1").await;

    let res = call(
        &app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        Some(json!({ "employee_id": emp })),
    )
    .await;
    assert_eq!(res.status, 201);
    let m = res.json();
    assert_eq!(keys(&m), measurement_keys());
    assert_eq!(m["status"], "started");
    assert_eq!(m["tenant_id"], tenant.to_string());
    assert_eq!(m["employee_id"], emp.to_string());
    assert!(m["alcohol_value"].is_null());
    assert!(m["result_type"].is_null());
    assert_eq!(m["device_use_count"], 0);

    // 不正な JSON は 400、欄の型違い (UUID でない) は 422 (axum の Json と同じ = rust と同じ)
    let res = call_raw(
        &app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        &[("content-type", "application/json")],
        b"{invalid json".to_vec(),
    )
    .await;
    assert_eq!(res.status, 400);
    let res = call(
        &app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        Some(json!({ "employee_id": "not-a-uuid" })),
    )
    .await;
    assert_eq!(res.status, 422);
}

#[tokio::test]
async fn test_create_measurement() {
    // POST /measurements — 201 と応答の形・既定値・alias・検証の 400
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "CR1").await;

    let res = call(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        Some(json!({
            "employee_id": emp,
            "alcohol_value": 0.12,
            "result_type": "over",
            "measured_at": "2026-09-12T01:00:00Z",
            "device_use_count": 7,
            "temperature": 36.6,
            "systolic": 121,
            "diastolic": 81,
            "pulse": 65,
            "medical_measured_at": "2026-09-12T00:59:00Z",
            "face_verified": true,
            "medical_manual_input": false,
            "face_photo_url": "https://example.invalid/f.jpg",
            "video_url": "https://example.invalid/v.webm",
        })),
    )
    .await;
    assert_eq!(res.status, 201);
    let m = res.json();
    assert_eq!(keys(&m), measurement_keys());
    assert_eq!(m["status"], "completed");
    assert_eq!(m["alcohol_value"], 0.12);
    assert_eq!(m["result_type"], "over");
    assert_eq!(m["measured_at"], "2026-09-12T01:00:00Z");
    assert_eq!(m["device_use_count"], 7);
    assert_eq!(m["temperature"], 36.6);
    assert_eq!(m["systolic"], 121);
    assert_eq!(m["diastolic"], 81);
    assert_eq!(m["pulse"], 65);
    assert_eq!(m["medical_measured_at"], "2026-09-12T00:59:00Z");
    assert_eq!(m["face_verified"], true);
    assert_eq!(m["medical_manual_input"], false);
    assert_eq!(m["face_photo_url"], "https://example.invalid/f.jpg");
    assert_eq!(m["video_url"], "https://example.invalid/v.webm");

    // alias (alcohol_level・result)。measured_at・device_use_count が無ければ NOW()・0
    let res = call(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        Some(json!({ "employee_id": emp, "alcohol_level": 0.0, "result": "pass" })),
    )
    .await;
    assert_eq!(res.status, 201);
    let m = res.json();
    assert_eq!(m["result_type"], "pass");
    assert_eq!(m["alcohol_value"], 0.0);
    assert_eq!(m["device_use_count"], 0);
    assert!(m["measured_at"].is_string());

    // 結果の 5 つ (VALID_RESULTS) は通り、それ以外は 400
    for (i, result) in ["pass", "fail", "normal", "over", "error"]
        .into_iter()
        .enumerate()
    {
        let body = measurement_body(emp, result, &format!("2026-09-13T0{i}:00:00Z"), false, None);
        let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
        assert_eq!(res.status, 201, "result={result}");
    }
    let before = measurement_count(tenant).await;
    let body = measurement_body(emp, "invalid", "2026-09-13T09:00:00Z", false, None);
    let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 400);
    // 種別の 3 つ (VALID_TENKO_TYPES) と不正な種別
    for (i, tt) in ["normal", "pre_operation", "post_operation"]
        .into_iter()
        .enumerate()
    {
        let body = measurement_body(
            emp,
            "pass",
            &format!("2026-09-14T0{i}:00:00Z"),
            false,
            Some(tt),
        );
        let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
        assert_eq!(res.status, 201, "tenko_type={tt}");
    }
    let body = measurement_body(emp, "pass", "2026-09-14T09:00:00Z", false, Some("bogus"));
    let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 400);
    // 電子車検証の番号の形
    let mut body = measurement_body(emp, "pass", "2026-09-14T10:00:00Z", false, None);
    body["carins_cert_no"] = Value::from("12345");
    let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 400);
    assert_eq!(
        measurement_count(tenant).await,
        before + 3,
        "400 の POST は作らない"
    );

    // 不正な JSON は 400、必須の欄が無いのは 422
    let res = call_raw(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        &[("content-type", "application/json")],
        b"{invalid json".to_vec(),
    )
    .await;
    assert_eq!(res.status, 400);
    let res = call(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        Some(json!({ "employee_id": emp })),
    )
    .await;
    assert_eq!(res.status, 422);
}

#[tokio::test]
async fn test_update_measurement() {
    // PUT /measurements/{id} — 200 と応答の形 (Measurement + tenko_session_id)・部分の更新・検証の 400・404
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "UP1").await;
    let m_id = start_measurement(&app, tenant, emp).await;

    // 途中の保存 (status 無し): 来た欄だけ変わる
    let json1 = put_measurement_ok(
        &app,
        tenant,
        &m_id,
        &json!({ "temperature": 36.4, "systolic": 118 }),
    )
    .await;
    let mut expected = measurement_keys();
    expected.push("tenko_session_id".to_string());
    expected.sort();
    assert_eq!(
        keys(&json1),
        expected,
        "増えるのは tenko_session_id だけ (入れ子にならない)"
    );
    assert_eq!(json1["status"], "started");
    assert_eq!(json1["temperature"], 36.4);
    assert_eq!(json1["systolic"], 118);
    assert!(json1["tenko_session_id"].is_null());

    // 完了の保存: 前の欄は残り (COALESCE)、alias も効く
    let json2 = put_measurement_ok(
        &app,
        tenant,
        &m_id,
        &json!({
            "status": "completed",
            "alcohol_level": 0.0,
            "result": "pass",
            "measured_at": "2026-09-12T03:00:00Z",
            "device_use_count": 3,
            "diastolic": 79,
            "pulse": 60,
            "medical_measured_at": "2026-09-12T02:59:00Z",
            "face_verified": false,
            "medical_manual_input": true,
            "face_photo_url": "https://example.invalid/f.jpg",
            "video_url": "https://example.invalid/v.webm",
        }),
    )
    .await;
    assert_eq!(json2["status"], "completed");
    assert_eq!(json2["result_type"], "pass");
    assert_eq!(json2["alcohol_value"], 0.0);
    assert_eq!(json2["temperature"], 36.4, "来なかった欄はそのまま");
    assert_eq!(json2["systolic"], 118);
    assert_eq!(json2["diastolic"], 79);
    assert_eq!(json2["pulse"], 60);
    assert_eq!(json2["device_use_count"], 3);
    assert_eq!(json2["measured_at"], "2026-09-12T03:00:00Z");
    assert_eq!(json2["medical_measured_at"], "2026-09-12T02:59:00Z");
    assert_eq!(json2["face_verified"], false);
    assert_eq!(json2["medical_manual_input"], true);
    assert_eq!(json2["face_photo_url"], "https://example.invalid/f.jpg");
    assert_eq!(json2["video_url"], "https://example.invalid/v.webm");
    assert_eq!(json2["created_at"], json1["created_at"]);
    assert!(
        json2["tenko_session_id"].is_null(),
        "印が無ければ点呼にしない"
    );

    // 検証の 400 (欄ごと。各定数の外の値)。どれも測定を変えない
    let bad = [
        json!({ "result_type": "invalid" }),
        json!({ "status": "done" }),
        json!({ "tenko_type": "mid_operation" }),
        json!({ "tenko_method": "遠隔点呼" }),
        json!({ "identity_method": "face" }),
        json!({ "carins_cert_no": "1234" }),
        json!({ "carins_vehicle_id": "SHORT" }),
    ];
    for body in bad {
        let res = put_measurement(&app, tenant, &m_id, &body).await;
        assert_eq!(res.status, 400, "{body}");
    }
    // 各定数の中の値は通る
    for status in ["started", "completed"] {
        let res = put_measurement(&app, tenant, &m_id, &json!({ "status": status })).await;
        assert_eq!(res.status, 200, "status={status}");
    }
    for result in ["pass", "fail", "normal", "over", "error"] {
        let res = put_measurement(&app, tenant, &m_id, &json!({ "result_type": result })).await;
        assert_eq!(res.status, 200, "result_type={result}");
    }
    let res = put_measurement(
        &app,
        tenant,
        &m_id,
        &json!({ "tenko_type": "post_operation", "tenko_method": "IT点呼", "identity_method": "nfc_card",
                 "carins_cert_no": "", "carins_vehicle_id": "" }),
    )
    .await;
    assert_eq!(res.status, 200);

    // 無い id は 404
    let res = put_measurement(
        &app,
        tenant,
        &Uuid::new_v4().to_string(),
        &json!({ "status": "completed" }),
    )
    .await;
    assert_eq!(res.status, 404);
    // 不正な UUID の path は 400、不正な JSON は 400
    let res = put_measurement(
        &app,
        tenant,
        "not-a-uuid",
        &json!({ "status": "completed" }),
    )
    .await;
    assert_eq!(res.status, 400);
    let res = call_raw(
        &app,
        "PUT",
        &format!("/api/measurements/{m_id}"),
        Some(tenant),
        &[("content-type", "application/json")],
        b"{not valid json".to_vec(),
    )
    .await;
    assert_eq!(res.status, 400);
}

#[tokio::test]
async fn test_get_measurement() {
    // GET /measurements/{id} — 200 と応答の形・404・不正な UUID
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "GT1").await;
    let m = post_measurement(&app, tenant, emp, "pass", "2026-09-12T01:00:00Z", false).await;

    let res = call(
        &app,
        "GET",
        &format!("/api/measurements/{}", m["id"].as_str().unwrap()),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 200);
    let got = res.json();
    assert_eq!(keys(&got), measurement_keys());
    assert_eq!(got, m, "作成の応答と同じ");

    let res = call(
        &app,
        "GET",
        &format!("/api/measurements/{}", Uuid::new_v4()),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 404);
    let res = call(
        &app,
        "GET",
        "/api/measurements/not-a-uuid",
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 400);
}

fn ids(v: &Value) -> Vec<String> {
    v["measurements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn test_list_measurements() {
    // GET /measurements — 応答の形・page / per_page の既定と境界・絞り込み・並び
    let app = app().await;
    let tenant = tenant().await;
    let emp_a = employee(tenant, "A", "LS1").await;
    let emp_b = employee(tenant, "B", "LS2").await;

    // 空の一覧の形
    let res = call(&app, "GET", "/api/measurements", Some(tenant), None).await;
    assert_eq!(res.status, 200);
    assert_eq!(
        res.json(),
        json!({ "measurements": [], "total": 0, "page": 1, "per_page": 50 })
    );

    // 03:00 (A, pass) / 02:00 (B, fail) / 01:00 (A, over) と、started の 1 件 (A)
    let m3 = post_measurement(&app, tenant, emp_a, "pass", "2026-09-12T03:00:00Z", false).await;
    let m2 = post_measurement(&app, tenant, emp_b, "fail", "2026-09-12T02:00:00Z", false).await;
    let m1 = post_measurement(&app, tenant, emp_a, "over", "2026-09-12T01:00:00Z", false).await;
    let started = start_measurement(&app, tenant, emp_a).await;
    put_measurement_ok(
        &app,
        tenant,
        &started,
        &json!({ "measured_at": "2026-09-11T00:00:00Z" }),
    )
    .await;
    let id = |m: &Value| m["id"].as_str().unwrap().to_string();

    let get = |q: &'static str| {
        let app = &app;
        async move {
            let res = call(
                app,
                "GET",
                &format!("/api/measurements{q}"),
                Some(tenant),
                None,
            )
            .await;
            assert_eq!(res.status, 200, "{q}");
            res.json()
        }
    };

    // 既定: measured_at の新しい順、page 1・per_page 50
    let all = get("").await;
    assert_eq!(all["total"], 4);
    assert_eq!(
        (all["page"].clone(), all["per_page"].clone()),
        (json!(1), json!(50))
    );
    assert_eq!(ids(&all), vec![id(&m3), id(&m2), id(&m1), started.clone()]);
    assert_eq!(keys(&all["measurements"][0]), measurement_keys());

    // per_page の上限 100・page の下限 1
    let v = get("?per_page=500").await;
    assert_eq!(v["per_page"], 100);
    let v = get("?per_page=100").await;
    assert_eq!(v["per_page"], 100);
    let v = get("?page=0").await;
    assert_eq!(v["page"], 1);
    assert_eq!(ids(&v).len(), 4);
    let v = get("?page=-3").await;
    assert_eq!(v["page"], 1);

    // ページ: total はページに依らない
    let v = get("?per_page=2&page=2").await;
    assert_eq!(
        (v["total"].clone(), v["page"].clone(), v["per_page"].clone()),
        (json!(4), json!(2), json!(2))
    );
    assert_eq!(ids(&v), vec![id(&m1), started.clone()]);
    let v = get("?per_page=2&page=3").await;
    assert_eq!(v["total"], 4);
    assert!(ids(&v).is_empty());

    // 絞り込み (と alias)
    let q = format!("?employee_id={emp_b}");
    let res = call(
        &app,
        "GET",
        &format!("/api/measurements{q}"),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(ids(&res.json()), vec![id(&m2)]);
    assert_eq!(ids(&get("?result_type=pass").await), vec![id(&m3)]);
    assert_eq!(ids(&get("?result=over").await), vec![id(&m1)]);
    assert_eq!(ids(&get("?status=started").await), vec![started.clone()]);
    assert_eq!(
        ids(&get("?date_from=2026-09-12T02:00:00Z").await),
        vec![id(&m3), id(&m2)]
    );
    assert_eq!(
        ids(&get("?from=2026-09-12T00:00:00Z&to=2026-09-12T02:00:00Z").await),
        vec![id(&m2), id(&m1)]
    );
    assert_eq!(
        ids(&get("?date_to=2026-09-12T01:00:00Z&status=completed").await),
        vec![id(&m1)]
    );
    // 全部の条件を一緒に (値の位置がずれない)
    let q = format!(
        "?employee_id={emp_a}&result_type=pass&date_from=2026-09-12T00:00:00Z&date_to=2026-09-12T23:00:00Z&status=completed&per_page=1&page=1"
    );
    let res = call(
        &app,
        "GET",
        &format!("/api/measurements{q}"),
        Some(tenant),
        None,
    )
    .await;
    let v = res.json();
    assert_eq!(v["total"], 1);
    assert_eq!(ids(&v), vec![id(&m3)]);

    // query の型違いは 400 (axum の Query = rust と同じ)
    for q in [
        "?employee_id=not-a-uuid",
        "?page=abc",
        "?date_from=yesterday",
    ] {
        let res = call(
            &app,
            "GET",
            &format!("/api/measurements{q}"),
            Some(tenant),
            None,
        )
        .await;
        assert_eq!(res.status, 400, "{q}");
    }
}

#[tokio::test]
async fn test_device_dev_rows_are_separate() {
    // dev端末 (X-Device-Dev: 1) の測定は is_dev の行になり、dev の要求からだけ見える (rust の app.device_dev と同じ)。
    // 点呼の記録も dev の行になる
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "DV1").await;
    let dev = [("X-Device-Dev", "1"), ("content-type", "application/json")];

    let body = measurement_body(emp, "normal", "2026-09-12T01:00:00Z", true, None);
    let res = call_raw(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        &dev,
        serde_json::to_vec(&body).unwrap(),
    )
    .await;
    assert_eq!(res.status, 201);
    let m_id = res.json()["id"].as_str().unwrap().to_string();

    let su = admin().await;
    let is_dev: bool = su
        .query_one(
            "SELECT is_dev FROM alc_api.measurements WHERE id = $1",
            &[&Uuid::parse_str(&m_id).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(is_dev);
    let flags: Vec<(bool, bool)> = su
        .query(
            "SELECT s.is_dev, r.is_dev FROM alc_api.tenko_sessions s
             JOIN alc_api.tenko_records r ON r.session_id = s.id WHERE s.tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(flags, vec![(true, true)], "点呼の記録も dev の行");

    // dev でない要求からは見えない (一覧・取得・更新)
    let res = call(&app, "GET", "/api/measurements", Some(tenant), None).await;
    assert_eq!(res.json()["total"], 0);
    let res = call(
        &app,
        "GET",
        &format!("/api/measurements/{m_id}"),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 404);
    let res = put_measurement(&app, tenant, &m_id, &json!({ "temperature": 35.0 })).await;
    assert_eq!(res.status, 404);

    // dev の要求からは見える (開始・一覧・取得・更新)
    let res = call_raw(
        &app,
        "GET",
        "/api/measurements",
        Some(tenant),
        &dev,
        Vec::new(),
    )
    .await;
    assert_eq!(res.json()["total"], 1);
    let res = call_raw(
        &app,
        "GET",
        &format!("/api/measurements/{m_id}"),
        Some(tenant),
        &dev,
        Vec::new(),
    )
    .await;
    assert_eq!(res.status, 200);
    let res = call_raw(
        &app,
        "PUT",
        &format!("/api/measurements/{m_id}"),
        Some(tenant),
        &dev,
        serde_json::to_vec(&json!({ "temperature": 35.0 })).unwrap(),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["temperature"], 35.0);
    let res = call_raw(
        &app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        &dev,
        serde_json::to_vec(&json!({ "employee_id": emp })).unwrap(),
    )
    .await;
    assert_eq!(res.status, 201);
    let res = call_raw(
        &app,
        "GET",
        "/api/measurements",
        Some(tenant),
        &dev,
        Vec::new(),
    )
    .await;
    assert_eq!(res.json()["total"], 2);

    // "1" 以外は dev でない (kit の device_dev_from_headers)
    let res = call_raw(
        &app,
        "GET",
        "/api/measurements",
        Some(tenant),
        &[("X-Device-Dev", "true")],
        Vec::new(),
    )
    .await;
    assert_eq!(res.json()["total"], 0);
}

#[tokio::test]
async fn test_db_failure_is_500_with_fixed_log() {
    // DB に届かない (接続が切れた) ときは 5 口とも 500。ログは段と kind だけ
    let app = build(broken_client().await);
    let tenant = tenant().await;
    let emp = Uuid::new_v4();
    let id = Uuid::new_v4();
    let routes = [
        (
            "POST",
            "/api/measurements/start".to_string(),
            Some(json!({ "employee_id": emp })),
        ),
        (
            "POST",
            "/api/measurements".to_string(),
            Some(json!({ "employee_id": emp, "alcohol_value": 0.0, "result_type": "pass" })),
        ),
        (
            "PUT",
            format!("/api/measurements/{id}"),
            Some(json!({ "status": "completed" })),
        ),
        ("GET", "/api/measurements".to_string(), None),
        ("GET", format!("/api/measurements/{id}"), None),
    ];
    for (method, uri, body) in routes {
        let res = call(&app, method, &uri, Some(tenant), body).await;
        assert_eq!(res.status, 500, "{method} {uri}");
        assert!(res.body.is_empty(), "応答の本文は空 (rust と同じ)");
    }
    let logs = app.logs();
    let stages = ["start", "create", "update", "list", "get"];
    assert_eq!(logs.len(), stages.len(), "{logs:?}");
    for (log, stage) in logs.iter().zip(stages) {
        assert!(
            log.starts_with(&format!("measurements: measurement {stage} failed: db (")),
            "{log}"
        );
        assert!(!log.contains(&tenant.to_string()) && !log.contains(&id.to_string()));
    }
}
