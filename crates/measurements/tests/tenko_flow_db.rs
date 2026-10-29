//! 自動点呼・遠隔点呼の段と運行管理者の判定を実 DB で通す (Refs ippoan/rust-alc-api#680。ippoan/alc-tenko-worker の
//! tests/sessions_db.rs から移した。元は rust-alc-api の tests/tenko_* と mock_tenko の要点・device_dev_axis_test の点呼の検査)。
//! 接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。webhook は alc-tenko-worker の Service Binding の偽物が
//! 受けたものを見る (送信そのものは alc-tenko-worker の tests/sessions_db.rs の webhook の送信の検査)。

mod support;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{admin, broken_client, build_tenko, call_raw, tenant, App, Got, MockService};
use uuid::Uuid;

/// alc-tenko-worker の偽物 (202 で受ける) を付けた app
async fn app() -> App {
    build_tenko(support::rt_client().await, MockService::with(202, false))
}

/// 社員を 1 人作る (社員コードは乱数)
async fn employee(t: Uuid, name: &str) -> Uuid {
    support::employee(t, name, &Uuid::new_v4().simple().to_string()[..8]).await
}

/// 欄を足して打つ
async fn call_with(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> Got {
    let mut h = headers.to_vec();
    let bytes = match body {
        Some(v) => {
            h.push(("content-type", "application/json"));
            serde_json::to_vec(&v).unwrap()
        }
        None => Vec::new(),
    };
    call_raw(app, method, uri, tenant, &h, bytes).await
}

/// ログが固定の頭で始まり、識別子・名前・番号を含まないこと
fn assert_logs_clean(app: &App, secrets: &[String]) {
    for line in app.logs() {
        assert!(
            line.starts_with("measurements: "),
            "固定の頭で始まらないログ"
        );
        for s in secrets {
            assert!(!line.contains(s.as_str()), "ログに識別子・名前が出た");
        }
    }
}

const ADMIN_USER: &str = "0b9f5a52-6a1e-4c1b-9d2a-3f4e5d6c7b8a";

fn admin_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        ("X-User-ID", ADMIN_USER),
        ("X-User-Email", "admin@example.invalid"),
        ("X-User-Role", "admin"),
    ]
}

async fn send(app: &App, method: &str, uri: &str, t: Uuid, body: Option<Value>) -> Got {
    call_with(app, method, uri, Some(t), &[], body).await
}

async fn send_h(
    app: &App,
    method: &str,
    uri: &str,
    t: Uuid,
    h: &[(&str, &str)],
    body: Option<Value>,
) -> Got {
    call_with(app, method, uri, Some(t), h, body).await
}

/// セッションを 1 件作る (superuser で)。`extra` は列名 → SQL の値 (テストの固定値を直に書く)
async fn sess(
    t: Uuid,
    emp: Uuid,
    tenko_type: &str,
    method: &str,
    status: &str,
    extra: &[(&str, &str)],
) -> Uuid {
    let id = Uuid::new_v4();
    let mut extra = extra.to_vec();
    if !extra.iter().any(|(c, _)| *c == "started_at") {
        extra.push(("started_at", "NOW()"));
    }
    let cols: String = extra.iter().map(|(c, _)| format!(", {c}")).collect();
    let vals: String = extra.iter().map(|(_, v)| format!(", {v}")).collect();
    admin()
        .await
        .execute(
            &format!(
                "INSERT INTO alc_api.tenko_sessions (id, tenant_id, employee_id, tenko_type, tenko_method, status{cols}) \
                 VALUES ($1, $2, $3, $4, $5, $6{vals})"
            ),
            &[&id, &t, &emp, &tenko_type, &method, &status],
        )
        .await
        .unwrap();
    id
}

async fn schedule(t: Uuid, emp: Uuid, tenko_type: &str, instruction: Option<&str>) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.tenko_schedules (tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, instruction) \
             VALUES ($1, $2, $3, '運行 管理者', NOW(), $4) RETURNING id",
            &[&t, &emp, &tenko_type, &instruction],
        )
        .await
        .unwrap()
        .get(0)
}

async fn baseline(t: Uuid, emp: Uuid) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employee_health_baselines (tenant_id, employee_id, baseline_systolic, baseline_diastolic, \
                 baseline_temperature, systolic_tolerance, diastolic_tolerance, temperature_tolerance) \
             VALUES ($1, $2, 120, 80, 36.5, 10, 10, 0.5)",
            &[&t, &emp],
        )
        .await
        .unwrap();
}

async fn manager(t: Uuid, role: &str) -> Uuid {
    let id = employee(t, "判定 管理者").await;
    admin()
        .await
        .execute(
            &format!("UPDATE alc_api.employees SET role = ARRAY['{role}'] WHERE id = $1"),
            &[&id],
        )
        .await
        .unwrap();
    id
}

async fn records(session: Uuid) -> Vec<(String, String, Value, String)> {
    admin()
        .await
        .query(
            "SELECT status, tenko_method, record_data, record_hash FROM alc_api.tenko_records WHERE session_id = $1",
            &[&session],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// alc-tenko-worker へ渡した webhook (種類, 本文)。見た分は消す
fn events(app: &App) -> Vec<(String, Value)> {
    let mock = app.tenko.as_ref().expect("tenko の偽物");
    std::mem::take(&mut *mock.requests.lock().unwrap())
        .into_iter()
        .map(|r| {
            assert_eq!(r.path, "/internal/webhooks/fire");
            let v: Value = serde_json::from_str(&r.body).unwrap();
            (
                v["event_type"].as_str().unwrap().to_owned(),
                v["payload"].clone(),
            )
        })
        .collect()
}

/// 業務前の自動点呼を最後まで通す: 予定の消費 → 医療 (血圧必須) → 自己申告と安全判定 → 日常点検 → 携行品 → アルコール → 指示の確認。
/// 完了で記録 (record_data はセッションの JSON、hash はその SHA-256) を書き、webhook は積まない
#[tokio::test]
async fn pre_operation_auto_flow_writes_record() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 一郎").await;
    baseline(t, emp).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let item: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.carrying_items (tenant_id, item_name) VALUES ($1, '免許証') RETURNING id",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);

    let got = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await;
    assert_eq!(got.status, 201);
    let s = got.json();
    let id = s["id"].as_str().unwrap().to_owned();
    assert_eq!(
        (
            s["status"].clone(),
            s["tenko_method"].clone(),
            s["responsible_manager_name"].clone()
        ),
        (
            json!("medical_pending"),
            json!("自動点呼"),
            json!("運行 管理者")
        )
    );
    let consumed = admin()
        .await
        .query_one(
            "SELECT consumed, consumed_by_session_id::text FROM alc_api.tenko_schedules WHERE id = $1",
            &[&sch],
        )
        .await
        .unwrap();
    assert!(consumed.get::<_, bool>(0));
    assert_eq!(consumed.get::<_, Option<String>>(1), Some(id.clone()));
    // 消費済みの予定ではもう始められない
    let again = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await;
    assert_eq!(again.status, 404);

    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let bp = send(
        &app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6 })),
    )
    .await;
    assert_eq!(bp.status, 400);
    assert_eq!(bp.json()["error"], "bp_required");
    let med = send(
        &app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6, "systolic": 125, "diastolic": 82, "pulse": 70 })),
    )
    .await;
    assert_eq!(med.json()["status"], "self_declaration_pending");

    let decl = send(
        &app,
        "PUT",
        &uri("self-declaration"),
        t,
        Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
    )
    .await;
    let decl = decl.json();
    assert_eq!(decl["status"], "daily_inspection_pending");
    assert_eq!(decl["safety_judgment"]["status"], "pass");
    assert_eq!(decl["safety_judgment"]["medical_diffs"]["systolic_diff"], 5);
    assert_eq!(
        decl["self_declaration"]["declared_at"],
        "2026-10-22T22:00:00Z"
    );

    let ok = json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    let insp = send(&app, "PUT", &uri("daily-inspection"), t, Some(ok)).await;
    assert_eq!(insp.json()["status"], "carrying_items_pending");

    let missing = Uuid::new_v4();
    let carry = send(
        &app,
        "PUT",
        &uri("carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": item, "checked": true }, { "item_id": missing, "checked": false } ] })),
    )
    .await;
    // マスタに無い品目は外部キーで入らない (rust と同じく 500)
    assert_eq!(carry.status, 500);
    let carry = send(
        &app,
        "PUT",
        &uri("carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": item, "checked": true } ] })),
    )
    .await;
    let c = carry.json();
    assert_eq!(c["status"], "identity_verified");
    assert_eq!(
        c["carrying_items_checked"]["items"],
        json!([{ "item_id": item, "item_name": "免許証", "checked": true }])
    );

    let alc = send(
        &app,
        "PUT",
        &uri("alcohol"),
        t,
        Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 })),
    )
    .await;
    assert_eq!(alc.json()["status"], "instruction_pending");
    assert!(records(id.parse().unwrap()).await.is_empty());

    let done = send(&app, "PUT", &uri("instruction-confirm"), t, None).await;
    let done_json = done.json();
    assert_eq!(done_json["status"], "completed");
    let recs = records(id.parse().unwrap()).await;
    assert_eq!(recs.len(), 1);
    let (status, method, data, hash) = &recs[0];
    assert_eq!(
        (status.as_str(), method.as_str()),
        ("completed", "自動点呼")
    );
    // record_data は応答のセッションと同じ値、hash はその SHA-256
    assert_eq!(data, &done_json);
    assert_eq!(
        hash,
        &format!("{:x}", Sha256::digest(data.to_string().as_bytes()))
    );
    assert!(events(&app).is_empty());
}

/// 業務後の遠隔点呼 (予定なし): アルコール → 運行報告 (指示が無ければ完了して記録)。報告は webhook を積む。
/// 予定の指示があれば指示の確認へ進み、記録はまだ書かない
#[tokio::test]
async fn post_operation_report_and_webhook() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 二郎").await;
    let got = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "employee_id": emp, "tenko_type": "post_operation", "location": "本社" })),
    )
    .await;
    assert_eq!(got.status, 201);
    let s = got.json();
    assert_eq!(
        (
            s["status"].clone(),
            s["tenko_method"].clone(),
            s["schedule_id"].clone()
        ),
        (json!("identity_verified"), json!("遠隔点呼"), Value::Null)
    );
    let id = s["id"].as_str().unwrap().to_owned();
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    // 業務後は医療・自己申告・日常点検を受けない
    assert_eq!(
        send(&app, "PUT", &uri("medical"), t, Some(json!({})))
            .await
            .json()["error"],
        "invalid_tenko_type"
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("self-declaration"),
            t,
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("alcohol"),
            t,
            Some(json!({ "alcohol_result": "normal", "alcohol_value": 0.0 }))
        )
        .await
        .json()["status"],
        "report_pending"
    );

    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("report"),
            t,
            Some(json!({ "vehicle_road_status": " ", "driver_alternation": "なし" }))
        )
        .await
        .status,
        400
    );
    let report = json!({ "vehicle_road_status": "異常なし", "driver_alternation": "報告なし", "vehicle_road_audio_url": "https://audio.example.invalid/a.webm" });
    let r = send(&app, "PUT", &uri("report"), t, Some(report))
        .await
        .json();
    // 遠隔点呼は乗務員の側が終わっても閉じない: 判定待ちで、記録はまだ書かない
    assert_eq!(
        (r["status"].clone(), r["completed_at"].clone()),
        (json!("judgment_pending"), Value::Null)
    );
    assert_eq!(
        r["report_vehicle_road_audio_url"],
        "https://audio.example.invalid/a.webm"
    );
    assert!(records(id.parse().unwrap()).await.is_empty());
    let ev = events(&app);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].0, "report_submitted");
    assert_eq!(ev[0].1["data"]["vehicle_road_status"], "異常なし");
    assert_eq!(ev[0].1["timestamp"], "2026-10-22T22:00:00Z");
    // 二度目は状態が違う
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("report"),
            t,
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" }))
        )
        .await
        .status,
        400
    );

    let gone = format!("/api/tenko/sessions/{}/report", Uuid::new_v4());
    assert_eq!(
        send(
            &app,
            "PUT",
            &gone,
            t,
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" }))
        )
        .await
        .status,
        404
    );
    // 予定に指示があれば指示の確認へ (記録はまだ)
    let sch = schedule(t, emp, "post_operation", Some("給油")).await;
    let s2 = sess(
        t,
        emp,
        "post_operation",
        "自動点呼",
        "report_pending",
        &[("schedule_id", &format!("'{sch}'"))],
    )
    .await;
    let r2 = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s2}/report"),
        t,
        Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" })),
    )
    .await
    .json();
    assert_eq!(
        (r2["status"].clone(), r2["completed_at"].clone()),
        (json!("instruction_pending"), Value::Null)
    );
    assert!(records(s2).await.is_empty());
    let done = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s2}/instruction-confirm"),
        t,
        None,
    )
    .await
    .json();
    assert_eq!(done["status"], "completed");
    let recs = records(s2).await;
    assert_eq!(
        recs[0].2["instruction_confirmed_at"],
        done["instruction_confirmed_at"]
    );
}

/// アルコール検知は中止して記録を書き、webhook を積む。dev端末の要求では積まない (dev の行は本番から見えない)
#[tokio::test]
async fn alcohol_detected_cancels_and_dev_does_not_queue() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 三郎").await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[("responsible_manager_name", "'管理 太郎'")],
    )
    .await;
    let uri = format!("/api/tenko/sessions/{id}/alcohol");
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "alcohol_result": "bogus", "alcohol_value": 0.1 }))
        )
        .await
        .status,
        400
    );
    let s = send(&app, "PUT", &uri, t, Some(json!({ "alcohol_result": "fail", "alcohol_value": 0.25, "alcohol_face_photo_url": "https://p.example.invalid/f.jpg" }))).await.json();
    assert_eq!(
        (
            s["status"].clone(),
            s["cancel_reason"].clone(),
            s["completed_at"].clone()
        ),
        (
            json!("cancelled"),
            json!("アルコール検知"),
            json!("2026-10-22T22:00:00Z")
        )
    );
    assert_eq!(records(id).await.len(), 1);
    let ev = events(&app);
    assert_eq!(ev[0].0, "alcohol_detected");
    assert_eq!(ev[0].1["data"]["employee_name"], "点呼 三郎");
    assert_eq!(ev[0].1["data"]["alcohol_value"], 0.25);
    assert_eq!(ev[0].1["data"]["responsible_manager_name"], "管理 太郎");
    // 状態が違う・無い
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        400
    );
    let none = format!("/api/tenko/sessions/{}/alcohol", Uuid::new_v4());
    assert_eq!(
        send(
            &app,
            "PUT",
            &none,
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        404
    );
    // 通常点呼 (種類が業務前・業務後でない) の結果は rust と同じく 500
    let normal = sess(t, emp, "normal", "通常点呼", "identity_verified", &[]).await;
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{normal}/alcohol"),
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        500
    );

    // dev端末: 行は dev の軸に入り、webhook は積まない。本番の要求からは見えない
    let dev = [("X-Device-Dev", "1")];
    let started = send_h(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        &dev,
        Some(json!({ "employee_id": emp, "tenko_type": "post_operation" })),
    )
    .await
    .json();
    let did = started["id"].as_str().unwrap().to_owned();
    let alc = send_h(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{did}/alcohol"),
        t,
        &dev,
        Some(json!({ "alcohol_result": "over", "alcohol_value": 0.3 })),
    )
    .await
    .json();
    assert_eq!(alc["status"], "cancelled");
    assert!(events(&app).is_empty());
    let is_dev: bool = admin()
        .await
        .query_one(
            "SELECT is_dev FROM alc_api.tenko_sessions WHERE id = $1",
            &[&did.parse::<Uuid>().unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(is_dev);
    assert_eq!(
        send(&app, "GET", &format!("/api/tenko/sessions/{did}"), t, None)
            .await
            .status,
        404
    );
    assert_eq!(
        send_h(
            &app,
            "GET",
            &format!("/api/tenko/sessions/{did}"),
            t,
            &dev,
            None
        )
        .await
        .status,
        200
    );
}

/// 安全判定の失敗 (基準値の外・自己申告) は中断して記録と webhook。管理者の再開・手動の中断・中止の検査
#[tokio::test]
async fn safety_judgment_interrupt_resume_and_cancel() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 四郎").await;
    baseline(t, emp).await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "self_declaration_pending",
        &[("systolic", "150"), ("temperature", "36.4")],
    )
    .await;
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let s = send(
        &app,
        "PUT",
        &uri("self-declaration"),
        t,
        Some(json!({ "illness": true, "fatigue": false, "sleep_deprivation": false })),
    )
    .await
    .json();
    assert_eq!(s["status"], "interrupted");
    assert_eq!(
        s["safety_judgment"]["failed_items"],
        json!(["systolic", "illness"])
    );
    assert_eq!(s["interrupted_at"], "2026-10-22T22:00:00Z");
    assert_eq!(records(id).await.len(), 1);
    let ev = events(&app);
    assert_eq!(
        (ev[0].0.as_str(), ev[0].1["data"]["failed_items"].clone()),
        ("tenko_interrupted", json!(["systolic", "illness"]))
    );

    // 管理者の再開: 理由が必須・AuthUser が要る (無いと rust と同じく 500)・中断中だけ
    assert_eq!(
        send_h(
            &app,
            "POST",
            &uri("resume"),
            t,
            &admin_headers(),
            Some(json!({ "reason": " " }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &uri("resume"),
            t,
            Some(json!({ "reason": "確認済み" }))
        )
        .await
        .status,
        500
    );
    let r = send_h(
        &app,
        "POST",
        &uri("resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "確認済み" })),
    )
    .await
    .json();
    assert_eq!(
        (r["status"].clone(), r["resumed_by_user_id"].clone()),
        (json!("daily_inspection_pending"), json!(ADMIN_USER))
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &uri("resume"),
            t,
            &admin_headers(),
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        400
    );
    let gone = format!("/api/tenko/sessions/{}/resume", Uuid::new_v4());
    assert_eq!(
        send_h(
            &app,
            "POST",
            &gone,
            t,
            &admin_headers(),
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        404
    );
    // 日常点検は済み・自己申告が無い中断は自己申告へ戻す
    let s2 = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "interrupted",
        &[("daily_inspection", "'{}'")],
    )
    .await;
    let r2 = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s2}/resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(r2["status"], "self_declaration_pending");
    let s3 = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "interrupted",
        &[("daily_inspection", "'{}'"), ("self_declaration", "'{}'")],
    )
    .await;
    let r3 = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s3}/resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(r3["status"], "daily_inspection_pending");

    // 手動の中断 (webhook を積む)。終端は弾く
    let i = send(
        &app,
        "POST",
        &uri("interrupt"),
        t,
        Some(json!({ "reason": "体調" })),
    )
    .await
    .json();
    assert_eq!(
        (i["status"].clone(), i["cancel_reason"].clone()),
        (json!("interrupted"), json!("体調"))
    );
    assert_eq!(events(&app)[0].1["data"]["reason"], "体調");
    assert_eq!(
        send(&app, "POST", &uri("interrupt"), t, Some(json!({})))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{}/interrupt", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .status,
        404
    );

    // 中止 (記録を書く)。完了・中止済みは弾く
    let c = send(
        &app,
        "POST",
        &uri("cancel"),
        t,
        Some(json!({ "reason": "取りやめ" })),
    )
    .await
    .json();
    assert_eq!(
        (c["status"].clone(), c["cancel_reason"].clone()),
        (json!("cancelled"), json!("取りやめ"))
    );
    assert_eq!(records(id).await.len(), 2);
    assert_eq!(
        send(&app, "POST", &uri("cancel"), t, Some(json!({})))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{}/cancel", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .status,
        404
    );

    // 基準値が無ければ体調は見ずに通す (ログに残す)
    let emp2 = employee(t, "点呼 五郎").await;
    let s4 = sess(
        t,
        emp2,
        "pre_operation",
        "自動点呼",
        "self_declaration_pending",
        &[("systolic", "200")],
    )
    .await;
    let p = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s4}/self-declaration"),
        t,
        Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
    )
    .await
    .json();
    assert_eq!(
        (p["status"].clone(), p["safety_judgment"]["status"].clone()),
        (json!("daily_inspection_pending"), json!("pass"))
    );
    assert!(app.logs().contains(
        &"measurements: no health baseline, safety judgment defaults to pass".to_owned()
    ));
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/self-declaration", Uuid::new_v4()),
            t,
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false }))
        )
        .await
        .status,
        404
    );
}

/// 日常点検: 値は ok / ng だけ。NG は中止・記録・webhook。携行品のマスタが無ければ本人確認済みへ
#[tokio::test]
async fn daily_inspection_and_carrying_items() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 六郎").await;
    let all = |v: &str| json!({ "brakes": v, "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "daily_inspection_pending",
        &[],
    )
    .await;
    let uri = format!("/api/tenko/sessions/{id}/daily-inspection");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(all("bad"))).await.status,
        400
    );
    let ok = send(&app, "PUT", &uri, t, Some(all("ok"))).await.json();
    assert_eq!(ok["status"], "identity_verified");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(all("ok"))).await.status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/daily-inspection", Uuid::new_v4()),
            t,
            Some(all("ok"))
        )
        .await
        .status,
        404
    );

    let ng = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "daily_inspection_pending",
        &[],
    )
    .await;
    let s = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{ng}/daily-inspection"),
        t,
        Some(all("ng")),
    )
    .await
    .json();
    assert_eq!(
        (s["status"].clone(), s["cancel_reason"].clone()),
        (json!("cancelled"), json!("日常点検異常"))
    );
    assert_eq!(s["daily_inspection"]["brakes"], "ng");
    assert_eq!(records(ng).await.len(), 1);
    let ev = events(&app);
    assert_eq!(
        (
            ev[0].0.as_str(),
            ev[0].1["data"]["inspection"]["brakes"].clone()
        ),
        ("inspection_ng", json!("ng"))
    );

    // 携行品: 状態の検査と、マスタに無い (別のテナントの) 品目の名前は空
    let c = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "carrying_items_pending",
        &[],
    )
    .await;
    let other = tenant().await;
    let foreign: Uuid = admin()
        .await
        .query_one("INSERT INTO alc_api.carrying_items (tenant_id, item_name) VALUES ($1, '他社') RETURNING id", &[&other])
        .await
        .unwrap()
        .get(0);
    let got = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{c}/carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": foreign, "checked": false } ] })),
    )
    .await
    .json();
    assert_eq!(got["carrying_items_checked"]["items"][0]["item_name"], "");
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{c}/carrying-items"),
            t,
            Some(json!({ "checks": [] }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/carrying-items", Uuid::new_v4()),
            t,
            Some(json!({ "checks": [] }))
        )
        .await
        .status,
        404
    );
}

/// 遠隔点呼への切り替え・医療と指示の確認の検査
#[tokio::test]
async fn escalate_medical_and_instruction_guards() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 七郎").await;
    let id = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let uri = format!("/api/tenko/sessions/{id}/escalate-remote");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(json!({ "reason": "  " })))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "reason": "x".repeat(201) }))
        )
        .await
        .status,
        400
    );
    let e = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "reason": " 血圧計の故障 " })),
    )
    .await
    .json();
    assert_eq!(
        (
            e["tenko_method"].clone(),
            e["remote_escalation_reason"].clone(),
            e["status"].clone()
        ),
        (
            json!("遠隔点呼"),
            json!("血圧計の故障"),
            json!("medical_pending")
        )
    );
    // 遠隔点呼は血圧なしで医療を受ける
    let m = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{id}/medical"),
        t,
        Some(json!({ "temperature": 36.5, "medical_manual_input": true })),
    )
    .await
    .json();
    assert_eq!(
        (m["status"].clone(), m["medical_manual_input"].clone()),
        (json!("self_declaration_pending"), json!(true))
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{id}/medical"),
            t,
            Some(json!({}))
        )
        .await
        .json()["error"],
        "invalid_session_status"
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/medical", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .json(),
        json!({ "error": "session_not_found" })
    );
    // 二重の切り替え・終了済み・無い
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(json!({ "reason": "x" })))
            .await
            .status,
        400
    );
    let done = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{done}/escalate-remote"),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/escalate-remote", Uuid::new_v4()),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        404
    );
    // 指示の確認は指示待ちだけ
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{done}/instruction-confirm"),
            t,
            None
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/instruction-confirm", Uuid::new_v4()),
            t,
            None
        )
        .await
        .status,
        404
    );
    // 予定の乗務員が違うと 400、無い予定は 404
    let other = employee(t, "点呼 別人").await;
    let sch = schedule(t, other, "pre_operation", Some("安全運転")).await;
    assert_eq!(
        send(
            &app,
            "POST",
            "/api/tenko/sessions/start",
            t,
            Some(json!({ "schedule_id": sch, "employee_id": emp }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            "/api/tenko/sessions/start",
            t,
            Some(json!({ "schedule_id": Uuid::new_v4(), "employee_id": emp }))
        )
        .await
        .status,
        404
    );
}

/// キオスクの自己再開: 理由が必須・終端は弾く・1 回まで・AuthUser が無ければ再開した人は空
#[tokio::test]
async fn self_resume_rules() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 八郎").await;
    let id = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let uri = format!("/api/tenko/sessions/{id}/self-resume");
    assert_eq!(
        send(&app, "POST", &uri, t, Some(json!({ "reason": " " })))
            .await
            .json()["error"],
        "reason_required"
    );
    let r = send(
        &app,
        "POST",
        &uri,
        t,
        Some(json!({ "reason": " 電池切れ " })),
    )
    .await
    .json();
    assert_eq!(
        (
            r["resume_reason"].clone(),
            r["resumed_by_user_id"].clone(),
            r["status"].clone()
        ),
        (json!("電池切れ"), Value::Null, json!("medical_pending"))
    );
    assert_eq!(
        send(&app, "POST", &uri, t, Some(json!({ "reason": "x" })))
            .await
            .json()["error"],
        "already_resumed"
    );
    let s2 = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let by = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s2}/self-resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(by["resumed_by_user_id"], ADMIN_USER);
    let done = sess(t, emp, "pre_operation", "自動点呼", "interrupted", &[]).await;
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{done}/self-resume"),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .json()["error"],
        "session_not_resumable"
    );
    let gone = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{}/self-resume", Uuid::new_v4()),
        t,
        Some(json!({ "reason": "x" })),
    )
    .await;
    assert_eq!(
        (gone.status, gone.json()),
        (404, json!({ "error": "session_not_found" }))
    );
}

/// 運行管理者の判定 (rust の device_dev_axis_test の点呼の検査を含む)
#[tokio::test]
async fn manager_judgment_rules() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 九郎").await;
    let judge = manager(t, "manager").await;
    let driver_only = manager(t, "driver").await;
    let auto = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    let it = sess(t, emp, "pre_operation", "IT点呼", "completed", &[]).await;
    let key = [("X-Device-Role", "device-tenko-manager")];
    let dev_key = [
        ("X-Device-Role", "device-tenko-manager"),
        ("X-Device-Dev", "1"),
    ];
    let body = |j: &str, by: Uuid, m: Option<&str>| json!({ "judgment": j, "judged_by_employee_id": by, "reason": "確認", "method": m });
    let u = |id: Uuid| format!("/api/tenko/sessions/{id}/judgment");

    // 管理者ログインでも運行管理者の鍵でもない → 401。判定の値・判定した人の検査
    assert_eq!(
        send(&app, "POST", &u(auto), t, Some(body("ok", judge, None)))
            .await
            .status,
        401
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("maybe", judge, None))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", Uuid::new_v4(), None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", driver_only, None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(Uuid::new_v4()),
            t,
            &admin_headers(),
            Some(body("ok", judge, None))
        )
        .await
        .status,
        404
    );
    // 管理者は点呼の方法で絞らない (自動点呼に確認の方法は付けない)
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", judge, Some("it")))
        )
        .await
        .status,
        400
    );
    let a = send_h(
        &app,
        "POST",
        &u(auto),
        t,
        &admin_headers(),
        Some(body("ng", judge, None)),
    )
    .await
    .json();
    assert_eq!(
        (
            a["manager_judgment"].clone(),
            a["manager_judgment_by"].clone()
        ),
        (json!("ng"), json!(judge))
    );
    // dev でない運行管理者の鍵は IT点呼 だけ。IT点呼 は確認の方法が要る (入っていれば押し直しで省ける)
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &key,
            Some(body("ok", judge, None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(&app, "POST", &u(it), t, &key, Some(body("ok", judge, None)))
            .await
            .status,
        400
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(it),
            t,
            &key,
            Some(body("ok", judge, Some("phone")))
        )
        .await
        .status,
        400
    );
    let k = send_h(
        &app,
        "POST",
        &u(it),
        t,
        &key,
        Some(body("ok", judge, Some("in_person"))),
    )
    .await
    .json();
    assert_eq!(k["manager_judgment_method"], "in_person");
    let again = send_h(&app, "POST", &u(it), t, &key, Some(body("ng", judge, None)))
        .await
        .json();
    assert_eq!(
        (
            again["manager_judgment"].clone(),
            again["manager_judgment_method"].clone()
        ),
        (json!("ng"), json!("in_person"))
    );
    // dev の運行管理者の鍵は dev のセッションだけ見え、点呼の方法で絞らない
    let dev_auto = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[("is_dev", "true")],
    )
    .await;
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &dev_key,
            Some(body("ok", judge, None))
        )
        .await
        .status,
        404
    );
    let d = send_h(
        &app,
        "POST",
        &u(dev_auto),
        t,
        &dev_key,
        Some(body("ok", judge, None)),
    )
    .await
    .json();
    assert_eq!(d["manager_judgment"], "ok");
}

/// 一覧の絞り込みと頁、dev の軸
#[tokio::test]
async fn list_filters_and_paging() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let emp = employee(t, "一覧 一郎").await;
    let emp2 = employee(t, "一覧 二郎").await;
    let a = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[
            ("started_at", "'2026-10-01T00:00:00Z'"),
            ("created_at", "'2026-10-01T00:00:00Z'"),
        ],
    )
    .await;
    let b = sess(
        t,
        emp2,
        "post_operation",
        "IT点呼",
        "completed",
        &[
            ("started_at", "'2026-10-02T00:00:00Z'"),
            ("created_at", "'2026-10-02T00:00:00Z'"),
            ("manager_judgment", "'ok'"),
        ],
    )
    .await;
    let c = sess(
        t,
        emp,
        "pre_operation",
        "遠隔点呼",
        "interrupted",
        &[
            ("started_at", "'2026-10-03T00:00:00Z'"),
            ("created_at", "'2026-10-03T00:00:00Z'"),
        ],
    )
    .await;
    sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[("is_dev", "true")],
    )
    .await;
    let oe = employee(other, "他社").await;
    sess(other, oe, "pre_operation", "自動点呼", "completed", &[]).await;

    let ids = |v: &Value| -> Vec<String> {
        v["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let all = send(&app, "GET", "/api/tenko/sessions", t, None)
        .await
        .json();
    assert_eq!(
        (
            ids(&all),
            all["total"].clone(),
            all["page"].clone(),
            all["per_page"].clone()
        ),
        (
            vec![c.to_string(), b.to_string(), a.to_string()],
            json!(3),
            json!(1),
            json!(50)
        )
    );
    for (q, want) in [
        (format!("employee_id={emp}"), vec![c, a]),
        ("status=completed".to_owned(), vec![b, a]),
        ("tenko_type=post_operation".to_owned(), vec![b]),
        (
            "tenko_method=%E9%81%A0%E9%9A%94%E7%82%B9%E5%91%BC".to_owned(),
            vec![c],
        ),
        ("judgment_pending=true".to_owned(), vec![c, a]),
        ("judgment_pending=false".to_owned(), vec![c, b, a]),
        (
            "date_from=2026-10-02T00:00:00Z&date_to=2026-10-02T12:00:00Z".to_owned(),
            vec![b],
        ),
    ] {
        let got = send(&app, "GET", &format!("/api/tenko/sessions?{q}"), t, None)
            .await
            .json();
        assert_eq!(
            ids(&got),
            want.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "{q}"
        );
    }
    let page = send(
        &app,
        "GET",
        "/api/tenko/sessions?page=2&per_page=2",
        t,
        None,
    )
    .await
    .json();
    assert_eq!(
        (ids(&page), page["total"].clone()),
        (vec![a.to_string()], json!(3))
    );
    let dev = send_h(
        &app,
        "GET",
        "/api/tenko/sessions",
        t,
        &[("X-Device-Dev", "1")],
        None,
    )
    .await
    .json();
    assert_eq!(dev["total"], 1);
    assert_eq!(
        send(
            &app,
            "GET",
            &format!("/api/tenko/sessions/{a}"),
            other,
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        support::call(&app, "GET", "/api/tenko/sessions", None, None)
            .await
            .status,
        401
    );
}

/// 記録の書き込みが失敗しても区切りだけ戻し、セッションの更新は残す (rust の `let _ = create_tenko_record(..)`)
#[tokio::test]
async fn record_failure_keeps_session_update() {
    reject_triggers().await;
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "記録 失敗").await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "medical_pending",
        &[("location", "'TEST-REJECT-RECORD'")],
    )
    .await;
    let c = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id}/cancel"),
        t,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        (c.status, c.json()["status"].clone()),
        (200, json!("cancelled"))
    );
    assert!(records(id).await.is_empty());
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("measurements: tenko record failed: body (")));
    assert_logs_clean(&app, &[id.to_string(), t.to_string()]);
}

/// DB の失敗は 500。理由付きの口 (医療・自己再開) は `{"error":"internal_error"}`、ほかは空
#[tokio::test]
async fn db_failures_are_500() {
    let app = build_tenko(broken_client().await, MockService::with(202, false));
    let t = Uuid::new_v4();
    let id = Uuid::new_v4();
    let s = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", "/api/tenko/sessions".into(), None, "session list"),
        (
            "GET",
            format!("/api/tenko/sessions/{id}"),
            None,
            "session get",
        ),
        (
            "POST",
            "/api/tenko/sessions/start".into(),
            Some(json!({ "employee_id": id })),
            "session start",
        ),
        (
            "PUT",
            s("alcohol"),
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 })),
            "session alcohol",
        ),
        (
            "PUT",
            s("escalate-remote"),
            Some(json!({ "reason": "x" })),
            "session escalate",
        ),
        ("PUT", s("instruction-confirm"), None, "session instruction"),
        (
            "PUT",
            s("report"),
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" })),
            "session report",
        ),
        ("POST", s("cancel"), Some(json!({})), "session cancel"),
        (
            "PUT",
            s("self-declaration"),
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
            "session self-declaration",
        ),
        (
            "PUT",
            s("daily-inspection"),
            Some(
                json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" }),
            ),
            "session daily-inspection",
        ),
        (
            "PUT",
            s("carrying-items"),
            Some(json!({ "checks": [] })),
            "session carrying-items",
        ),
        ("POST", s("interrupt"), Some(json!({})), "session interrupt"),
    ];
    for (method, uri, body, stage) in cases {
        let got = call_with(&app, method, &uri, Some(t), &[], body).await;
        assert_eq!((got.status, got.body.is_empty()), (500, true), "{stage}");
        assert!(
            app.logs()
                .iter()
                .any(|l| l.starts_with(&format!("measurements: {stage} failed: db ("))),
            "{stage}"
        );
    }
    let r = call_with(
        &app,
        "POST",
        &s("resume"),
        Some(t),
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await;
    assert_eq!(r.status, 500);
    let j = call_with(
        &app,
        "POST",
        &s("judgment"),
        Some(t),
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": id })),
    )
    .await;
    assert_eq!(j.status, 500);
    for (uri, body) in [
        (s("medical"), json!({})),
        (s("self-resume"), json!({ "reason": "x" })),
    ] {
        let got = call_with(
            &app,
            if uri.ends_with("medical") {
                "PUT"
            } else {
                "POST"
            },
            &uri,
            Some(t),
            &[],
            Some(body),
        )
        .await;
        assert_eq!(
            (got.status, got.json()),
            (500, json!({ "error": "internal_error" }))
        );
    }
    assert_logs_clean(&app, &[id.to_string(), t.to_string()]);
}

/// 記録の書き込みで落とす trigger (location が `TEST-REJECT-RECORD` の記録だけ) と、段の更新で落とす trigger
/// (location が `TEST-REJECT-ALCOHOL` のセッションだけ)
async fn reject_triggers() {
    static DONE: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    DONE.get_or_init(|| async {
        admin()
            .await
            .batch_execute(
            "CREATE OR REPLACE FUNCTION alc_api.tenko_test_reject_record() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.location = 'TEST-REJECT-RECORD' THEN RAISE EXCEPTION 'test reject'; END IF; RETURN NEW; END $$; \
             DROP TRIGGER IF EXISTS tenko_test_reject_record ON alc_api.tenko_records; \
             CREATE TRIGGER tenko_test_reject_record BEFORE INSERT ON alc_api.tenko_records \
             FOR EACH ROW EXECUTE FUNCTION alc_api.tenko_test_reject_record(); \
             CREATE OR REPLACE FUNCTION alc_api.tenko_test_reject_alcohol() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.location = 'TEST-REJECT-ALCOHOL' AND NEW.alcohol_result IS NOT NULL THEN \
             RAISE EXCEPTION 'test reject'; END IF; RETURN NEW; END $$; \
             DROP TRIGGER IF EXISTS tenko_test_reject_alcohol ON alc_api.tenko_sessions; \
             CREATE TRIGGER tenko_test_reject_alcohol BEFORE UPDATE ON alc_api.tenko_sessions \
             FOR EACH ROW EXECUTE FUNCTION alc_api.tenko_test_reject_alcohol();",
            )
            .await
            .unwrap();
    })
    .await;
}

async fn session_status(id: Uuid) -> String {
    admin()
        .await
        .query_one(
            "SELECT status FROM alc_api.tenko_sessions WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}

/// 遠隔点呼は乗務員の側が終わると判定待ち。判定で閉じて記録を書き (判定が record_data と hash に入る)、記録の後は押し直せない
#[tokio::test]
async fn judgment_closes_pending_and_writes_record() {
    reject_triggers().await;
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "遠隔 太郎").await;
    let judge = manager(t, "manager").await;
    let u = |id: Uuid| format!("/api/tenko/sessions/{id}/judgment");
    let body = |j: &str, m: Option<&str>| json!({ "judgment": j, "judged_by_employee_id": judge, "reason": "通話で確認", "method": m });

    // 業務前の遠隔点呼: 指示の確認で判定待ち (記録はまだ)
    let pre = sess(
        t,
        emp,
        "pre_operation",
        "遠隔点呼",
        "instruction_pending",
        &[],
    )
    .await;
    let c = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{pre}/instruction-confirm"),
        t,
        None,
    )
    .await
    .json();
    assert_eq!(
        (c["status"].clone(), c["completed_at"].clone()),
        (json!("judgment_pending"), Value::Null)
    );
    assert!(c["instruction_confirmed_at"].is_string());
    assert!(records(pre).await.is_empty());

    // 判定待ちは乗務員の側から中止・中断・自己再開できない
    for (method, step, b) in [
        ("POST", "cancel", json!({})),
        ("POST", "interrupt", json!({})),
    ] {
        let got = send(
            &app,
            method,
            &format!("/api/tenko/sessions/{pre}/{step}"),
            t,
            Some(b),
        )
        .await;
        assert_eq!(got.status, 400, "{step}");
    }
    let r = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{pre}/self-resume"),
        t,
        Some(json!({ "reason": "再開" })),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (400, json!("session_not_resumable"))
    );

    // NG でも閉じる (中止にはしない)。記録の record_data に判定が入り、hash はその SHA-256
    let j = send_h(
        &app,
        "POST",
        &u(pre),
        t,
        &admin_headers(),
        Some(body("ng", None)),
    )
    .await;
    assert_eq!(j.status, 200);
    let j = j.json();
    assert_eq!(
        (j["status"].clone(), j["manager_judgment"].clone()),
        (json!("completed"), json!("ng"))
    );
    assert!(j["completed_at"].is_string());
    let recs = records(pre).await;
    assert_eq!(recs.len(), 1);
    let (status, method, data, hash) = &recs[0];
    assert_eq!(
        (status.as_str(), method.as_str()),
        ("completed", "遠隔点呼")
    );
    assert_eq!(data["manager_judgment"], "ng");
    assert_eq!(data["manager_judgment_by"], json!(judge));
    assert_eq!(
        hash,
        &format!("{:x}", Sha256::digest(data.to_string().as_bytes()))
    );
    // 記録の後は押し直せない
    let again = send_h(
        &app,
        "POST",
        &u(pre),
        t,
        &admin_headers(),
        Some(body("ok", None)),
    )
    .await;
    assert_eq!(
        (again.status, again.json()),
        (409, json!({ "error": "already_recorded" }))
    );
    assert_eq!(records(pre).await.len(), 1);

    // IT点呼の判定待ちは運行管理者の鍵で、確認の方法を付けて閉じる
    let key = [("X-Device-Role", "device-tenko-manager")];
    let it = sess(t, emp, "pre_operation", "IT点呼", "judgment_pending", &[]).await;
    assert_eq!(
        send_h(&app, "POST", &u(it), t, &key, Some(body("ok", None)))
            .await
            .status,
        400
    );
    let k = send_h(&app, "POST", &u(it), t, &key, Some(body("ok", Some("it"))))
        .await
        .json();
    assert_eq!(
        (k["status"].clone(), k["manager_judgment_method"].clone()),
        (json!("completed"), json!("it"))
    );
    assert_eq!(records(it).await[0].1, "IT点呼");

    // 自動点呼は判定を待たない: 業務後の運行報告 (予定の指示なし) で完了して記録を書く
    let auto = sess(t, emp, "post_operation", "自動点呼", "report_pending", &[]).await;
    let ar = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{auto}/report"),
        t,
        Some(json!({ "vehicle_road_status": "異常なし", "driver_alternation": "なし" })),
    )
    .await
    .json();
    assert_eq!(ar["status"], "completed");
    assert_eq!(records(auto).await.len(), 1);
    assert_eq!(events(&app)[0].0, "report_submitted");

    // 記録が書けなければ判定ごと戻す (判定待ちのまま残り、押し直せる)
    let broken = sess(
        t,
        emp,
        "post_operation",
        "遠隔点呼",
        "judgment_pending",
        &[("location", "'TEST-REJECT-RECORD'")],
    )
    .await;
    let f = send_h(
        &app,
        "POST",
        &u(broken),
        t,
        &admin_headers(),
        Some(body("ok", None)),
    )
    .await;
    assert_eq!(f.status, 500);
    assert_eq!(session_status(broken).await, "judgment_pending");
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("measurements: session judgment failed: db (")));
    assert!(events(&app).is_empty());
    assert_logs_clean(&app, &[t.to_string(), judge.to_string()]);
}

/// 測定の完了の保存 (`tenko_session_id`) でアルコールの段を進める: 値は測定の行から取り、再送は進め直さない
#[tokio::test]
async fn alcohol_step_rides_on_the_measurement_save() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "測定 一郎").await;
    let other = employee(t, "測定 二郎").await;
    let sid = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[],
    )
    .await;
    let m = support::start_measurement(&app, t, emp).await;
    let mut done = support::completed_body("normal", "2026-10-22T21:59:00Z", false, None, None);
    done["face_photo_url"] = json!("r2://alc-media/x.jpg");
    done["tenko_session_id"] = json!(sid);

    // 通常点呼の新しいセッションと一緒には付けられない
    let mut both = done.clone();
    both["record_as_tenko"] = json!(true);
    assert_eq!(
        support::put_measurement(&app, t, &m, &both).await.status,
        400
    );

    // 完了していない保存では進めない (今のセッションを返す)
    let mid = support::put_measurement_ok(
        &app,
        t,
        &m,
        &json!({ "temperature": 36.4, "tenko_session_id": sid }),
    )
    .await;
    assert_eq!(
        (
            mid["tenko_session"]["status"].clone(),
            mid["tenko_session_id"].clone()
        ),
        (json!("identity_verified"), json!(sid))
    );

    let r = support::put_measurement_ok(&app, t, &m, &done).await;
    assert_eq!(r["status"], "completed");
    let s = &r["tenko_session"];
    assert_eq!(
        (
            s["status"].clone(),
            s["measurement_id"].clone(),
            s["alcohol_result"].clone(),
            s["alcohol_value"].clone(),
            s["alcohol_face_photo_url"].clone(),
            s["alcohol_tested_at"].clone(),
        ),
        (
            json!("instruction_pending"),
            json!(m),
            json!("normal"),
            json!(0.0),
            json!("r2://alc-media/x.jpg"),
            json!("2026-10-22T21:59:00Z"),
        )
    );
    // 再送は進め直さない (同じセッションをそのまま返す)
    let again = support::put_measurement_ok(&app, t, &m, &done).await;
    assert_eq!(again["tenko_session"]["status"], "instruction_pending");
    assert_eq!(again["tenko_session"]["updated_at"], s["updated_at"]);
    assert!(events(&app).is_empty());

    // 合わないセッションは保存ごと 409 (測定は書かない)
    let m2 = support::start_measurement(&app, t, other).await;
    let fresh = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[],
    )
    .await;
    let medical = sess(
        t,
        other,
        "pre_operation",
        "自動点呼",
        "medical_pending",
        &[],
    )
    .await;
    for bad in [fresh, medical, Uuid::new_v4()] {
        let mut b = done.clone();
        b["tenko_session_id"] = json!(bad);
        assert_eq!(support::put_measurement(&app, t, &m2, &b).await.status, 409);
    }
    assert_eq!(support::measurement_status(&m2).await, "started");
    // 測定が無ければ 404 (セッションは見ない)
    assert_eq!(
        support::put_measurement(&app, t, &Uuid::new_v4().to_string(), &done)
            .await
            .status,
        404
    );

    // 結果が点呼にならない (error) なら本人確認済みのまま (測り直せる)
    let m3 = support::start_measurement(&app, t, emp).await;
    let mut err = done.clone();
    err["result_type"] = json!("error");
    err["tenko_session_id"] = json!(fresh);
    let e = support::put_measurement_ok(&app, t, &m3, &err).await;
    assert_eq!(e["tenko_session"]["status"], "identity_verified");
    assert_eq!(session_status(fresh).await, "identity_verified");

    // 種別が業務前・業務後でなければ進めない
    let normal = sess(t, emp, "normal", "自動点呼", "identity_verified", &[]).await;
    let m4 = support::start_measurement(&app, t, emp).await;
    let mut n = done.clone();
    n["tenko_session_id"] = json!(normal);
    let nr = support::put_measurement_ok(&app, t, &m4, &n).await;
    assert_eq!(nr["tenko_session"]["status"], "identity_verified");
}

/// 業務後で検知: 中止で閉じて記録を書き、アルコール検知の webhook を測定の値で渡す。dev の要求は渡さない
#[tokio::test]
async fn alcohol_detected_from_the_measurement() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "検知 三郎").await;
    let sid = sess(
        t,
        emp,
        "post_operation",
        "遠隔点呼",
        "identity_verified",
        &[],
    )
    .await;
    let m = support::start_measurement(&app, t, emp).await;
    let mut body = support::completed_body("over", "2026-10-22T21:58:00Z", false, None, None);
    body["alcohol_value"] = json!(0.25);
    body["tenko_session_id"] = json!(sid);
    let r = support::put_measurement_ok(&app, t, &m, &body).await;
    let s = &r["tenko_session"];
    assert_eq!(
        (s["status"].clone(), s["cancel_reason"].clone()),
        (json!("cancelled"), json!("アルコール検知"))
    );
    assert_eq!(s["completed_at"], "2026-10-22T22:00:00Z");
    let recs = records(sid).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, "cancelled");
    let ev = events(&app);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].0, "alcohol_detected");
    assert_eq!(
        (
            ev[0].1["tenant_id"].clone(),
            ev[0].1["data"]["alcohol_value"].clone(),
            ev[0].1["data"]["alcohol_result"].clone(),
            ev[0].1["data"]["employee_name"].clone(),
        ),
        (json!(t), json!(0.25), json!("over"), json!("検知 三郎"))
    );
    let mock = app.tenko.as_ref().unwrap();
    assert!(mock.requests().is_empty());

    // dev端末の要求は dev のセッションを進め、webhook は渡さない
    let dev_sid = sess(
        t,
        emp,
        "post_operation",
        "遠隔点呼",
        "identity_verified",
        &[("is_dev", "true")],
    )
    .await;
    let start = call_with(
        &app,
        "POST",
        "/api/measurements/start",
        Some(t),
        &[("X-Device-Dev", "1")],
        Some(json!({ "employee_id": emp })),
    )
    .await
    .json();
    let dm = start["id"].as_str().unwrap().to_owned();
    let mut dbody = body.clone();
    dbody["tenko_session_id"] = json!(dev_sid);
    let d = call_with(
        &app,
        "PUT",
        &format!("/api/measurements/{dm}"),
        Some(t),
        &[("X-Device-Dev", "1")],
        Some(dbody),
    )
    .await;
    assert_eq!(d.json()["tenko_session"]["status"], "cancelled");
    assert!(events(&app).is_empty());
    // dev の測定から本番のセッションは見えない
    let prod = sess(
        t,
        emp,
        "post_operation",
        "遠隔点呼",
        "identity_verified",
        &[],
    )
    .await;
    let start = call_with(
        &app,
        "POST",
        "/api/measurements/start",
        Some(t),
        &[("X-Device-Dev", "1")],
        Some(json!({ "employee_id": emp })),
    )
    .await
    .json();
    let mut pbody = body.clone();
    pbody["tenko_session_id"] = json!(prod);
    let p = call_with(
        &app,
        "PUT",
        &format!("/api/measurements/{}", start["id"].as_str().unwrap()),
        Some(t),
        &[("X-Device-Dev", "1")],
        Some(pbody),
    )
    .await;
    assert_eq!(p.status, 409);
}

/// 段を進めるのに失敗しても測定の保存は残す。webhook の渡し先が無い・断る・届かないときはログ 1 行
#[tokio::test]
async fn alcohol_failures_keep_the_measurement() {
    reject_triggers().await;
    let t = tenant().await;
    let emp = employee(t, "失敗 四郎").await;
    let body = |sid: Uuid| {
        let mut b = support::completed_body("fail", "2026-10-22T21:57:00Z", false, None, None);
        b["tenko_session_id"] = json!(sid);
        b
    };

    // 段の更新が落ちる → 区切りだけ戻す (測定は完了、セッションは本人確認済みのまま)
    let app = app().await;
    let sid = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[("location", "'TEST-REJECT-ALCOHOL'")],
    )
    .await;
    let m = support::start_measurement(&app, t, emp).await;
    let r = support::put_measurement_ok(&app, t, &m, &body(sid)).await;
    assert_eq!(r["status"], "completed");
    assert_eq!(r["tenko_session"]["status"], "identity_verified");
    assert_eq!(session_status(sid).await, "identity_verified");
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("measurements: tenko alcohol failed: body (")));
    assert!(events(&app).is_empty());

    // 渡し先が断る・届かない・binding が無い
    for (mock, line) in [
        (
            Some(MockService::with(500, false)),
            "measurements: tenko webhook alcohol_detected refused: status 500",
        ),
        (
            Some(MockService::with(202, true)),
            "measurements: tenko webhook alcohol_detected unreachable: fetch",
        ),
        (None, "measurements: tenko webhook skipped: no binding"),
    ] {
        let app = match mock {
            Some(mock) => build_tenko(support::rt_client().await, mock),
            None => support::build(support::rt_client().await),
        };
        let sid = sess(
            t,
            emp,
            "pre_operation",
            "自動点呼",
            "identity_verified",
            &[],
        )
        .await;
        let m = support::start_measurement(&app, t, emp).await;
        let r = support::put_measurement_ok(&app, t, &m, &body(sid)).await;
        assert_eq!(r["tenko_session"]["status"], "cancelled");
        assert!(app.logs().contains(&line.to_owned()), "{line}");
        assert_logs_clean(&app, &[t.to_string(), sid.to_string()]);
    }
}
