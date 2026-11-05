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

/// 測定を 1 件作り、完了の保存 (`tenko_session_id` 付き) でアルコールの段を進める。応答の `tenko_session` を返す
async fn measure(app: &App, t: Uuid, emp: Uuid, session: &str, result: &str) -> Value {
    let m = support::start_measurement(app, t, emp).await;
    let mut body = support::completed_body(result, "2026-10-22T21:59:00Z", false, None, None);
    body["tenko_session_id"] = json!(session);
    support::put_measurement_ok(app, t, &m, &body).await["tenko_session"].clone()
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
    // 値 (体温・血圧) は値の置き場の測定に入れ、セッションはそれを指す (セッションの値の列は読まれない)
    let (values, mut extra): (Vec<_>, Vec<_>) = extra.into_iter().partition(|(c, _)| {
        [
            "temperature",
            "systolic",
            "diastolic",
            "pulse",
            "medical_measured_at",
            "result",
        ]
        .contains(c)
    });
    if !values.is_empty() {
        let cols: String = values.iter().map(|(c, _)| format!(", {c}")).collect();
        let vals: String = values.iter().map(|(_, v)| format!(", {v}")).collect();
        let mid: Uuid = admin()
            .await
            .query_one(
                &format!(
                    "INSERT INTO alc_api.measurements (tenant_id, employee_id, status{cols}) \
                     VALUES ($1, $2, 'started'{vals}) RETURNING id"
                ),
                &[&t, &emp],
            )
            .await
            .unwrap()
            .get(0);
        extra.push((
            "measurement_id",
            Box::leak(format!("'{mid}'").into_boxed_str()),
        ));
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

/// 血圧の記録を `days` 日分 (東京の日付が異なる日。今日から 1 日ずつ遡る) 作る。自動点呼 (業務前) の開始が要る 10 日分の用意
async fn bp_history(t: Uuid, emp: Uuid, days: i32) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.measurements (tenant_id, employee_id, status, systolic, diastolic, medical_measured_at) \
             SELECT $1, $2, 'completed', 120, 80, NOW() - make_interval(days => d) FROM generate_series(1, $3) AS d",
            &[&t, &emp, &days],
        )
        .await
        .unwrap();
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
    bp_history(t, emp, 10).await;
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

    let got = send_h(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        &[("X-Device-Id", "kiosk-1")],
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await;
    assert_eq!(got.status, 201);
    let s = got.json();
    let id = s["id"].as_str().unwrap().to_owned();
    // 値の置き場の測定を作ってセッションから指す (started・乗務員と端末はセッションと同じ)
    let mid: Uuid = s["measurement_id"].as_str().unwrap().parse().unwrap();
    let m = admin()
        .await
        .query_one(
            "SELECT status, employee_id, device_id FROM alc_api.measurements WHERE id = $1 AND tenant_id = $2",
            &[&mid, &t],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            m.get::<_, String>(0),
            m.get::<_, Uuid>(1),
            m.get::<_, Option<String>>(2)
        ),
        ("started".to_owned(), emp, Some("kiosk-1".to_owned()))
    );
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
    let med = send(
        &app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6, "systolic": 125, "diastolic": 82, "pulse": 70 })),
    )
    .await;
    let med = med.json();
    assert_eq!(med["status"], "self_declaration_pending");
    // 体温・血圧は測定にも同じ値で入る
    let v = admin()
        .await
        .query_one(
            "SELECT temperature, systolic, diastolic, pulse, medical_measured_at::text, medical_manual_input \
             FROM alc_api.measurements WHERE id = $1",
            &[&mid],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            v.get::<_, Option<f64>>(0),
            v.get::<_, Option<i32>>(1),
            v.get::<_, Option<i32>>(2),
            v.get::<_, Option<i32>>(3),
            v.get::<_, Option<bool>>(5),
        ),
        (Some(36.6), Some(125), Some(82), Some(70), None)
    );
    assert!(v.get::<_, Option<String>>(4).is_some());
    assert_eq!(med["measurement_id"], json!(mid));

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

    let alc = measure(&app, t, emp, &id, "normal").await;
    assert_eq!(alc["status"], "instruction_pending");
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

/// 業務後の遠隔点呼 (予定なし): アルコール → 運行報告 (指示が無ければ判定待ち)。報告は webhook を積む。
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
        measure(&app, t, emp, &id, "normal").await["status"],
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

    // 管理者の再開: 理由が必須・管理者ログインか運行管理者の鍵が要る (キオスクの鍵は 401)・中断中だけ
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
        401
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
    // 2 回目も受ける (再開の時刻・理由は最初の値のまま。管理者が再開済みのセッションも受けるため)
    let again = send(&app, "POST", &uri, t, Some(json!({ "reason": "x" }))).await;
    assert_eq!(again.status, 200);
    assert_eq!(again.json()["resume_reason"], "電池切れ");
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
             BEGIN IF NEW.location = 'TEST-REJECT-ALCOHOL' AND NEW.measurement_id IS DISTINCT FROM OLD.measurement_id THEN \
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

    // 判定待ちは乗務員の側から中断・自己再開できない (取り消しは遠隔点呼なら下の
    // remote_judgment_pending_can_be_cancelled_by_driver)
    for (method, step, b) in [("POST", "interrupt", json!({}))] {
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
    // 1 件: 閉じたセッションは判定付きで読める。無い id なら 404
    let one = send(&app, "GET", &format!("/api/tenko/sessions/{it}"), t, None).await;
    assert_eq!(
        (one.status, one.json()["manager_judgment"].clone()),
        (200, json!("ok"))
    );
    let gone = format!("/api/tenko/sessions/{}", Uuid::new_v4());
    assert_eq!(send(&app, "GET", &gone, t, None).await.status, 404);

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
    // キオスクのアルコールの保存は体温を送らない (alc-app の completedMeasurementFields)
    body.as_object_mut().unwrap().remove("temperature");
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

/// 遠隔点呼の判定待ち (運行管理者を通話で呼んでいる間) は、運行管理者が出ないとき乗務員が取り消せる。
/// 取り消すと中止の記録を書く。IT点呼 の判定待ちは今までどおり取り消せない (2026-11-01 ユーザー決定)
#[tokio::test]
async fn remote_judgment_pending_can_be_cancelled_by_driver() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "遠隔 二郎").await;
    let pre = sess(t, emp, "pre_operation", "遠隔点呼", "judgment_pending", &[]).await;
    let got = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{pre}/cancel"),
        t,
        Some(json!({ "reason": "運行管理者が通話に出なかったため乗務員が中止" })),
    )
    .await;
    assert_eq!(got.status, 200);
    let s = got.json();
    assert_eq!(
        (s["status"].clone(), s["cancel_reason"].clone()),
        (
            json!("cancelled"),
            json!("運行管理者が通話に出なかったため乗務員が中止")
        )
    );
    let recs = records(pre).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].0, "cancelled");

    let it = sess(t, emp, "normal", "IT点呼", "judgment_pending", &[]).await;
    let got = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{it}/cancel"),
        t,
        Some(json!({ "reason": "x" })),
    )
    .await;
    assert_eq!(got.status, 400);
}

/// 運行管理者の点呼の一覧から送る取り消し・中断・再開 (運行管理者の鍵)。範囲は判定と同じ: 開発用の席の鍵はどの点呼も、
/// 本番の席の鍵は IT点呼 だけ (2026-11-02 ユーザー決定)。再開を送った人の列は席の鍵では NULL のまま
#[tokio::test]
async fn manager_key_cancel_interrupt_resume_scope() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "席 三郎").await;
    let key = [("X-Device-Role", "device-tenko-manager")];
    let dev_key = [
        ("X-Device-Role", "device-tenko-manager"),
        ("X-Device-Dev", "1"),
    ];
    let u = |id: Uuid, step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let reason = || Some(json!({ "reason": "席から", "resumed_by_name": "席 管理者" }));

    // 本番の席の鍵: IT点呼 でない点呼には触れない (403。行は変わらない)
    let remote = sess(t, emp, "pre_operation", "遠隔点呼", "medical_pending", &[]).await;
    for step in ["cancel", "interrupt"] {
        assert_eq!(
            send_h(&app, "POST", &u(remote, step), t, &key, reason())
                .await
                .status,
            403,
            "{step}"
        );
    }
    let stopped = sess(t, emp, "pre_operation", "自動点呼", "interrupted", &[]).await;
    assert_eq!(
        send_h(&app, "POST", &u(stopped, "resume"), t, &key, reason())
            .await
            .status,
        403
    );
    assert_eq!(session_status(remote).await, "medical_pending");
    assert_eq!(session_status(stopped).await, "interrupted");

    // 本番の席の鍵: IT点呼 は中断 → 再開 → 取り消しまで通る
    let it = sess(t, emp, "normal", "IT点呼", "self_declaration_pending", &[]).await;
    let i = send_h(&app, "POST", &u(it, "interrupt"), t, &key, reason())
        .await
        .json();
    assert_eq!(i["status"], "interrupted");
    let r = send_h(&app, "POST", &u(it, "resume"), t, &key, reason())
        .await
        .json();
    assert_eq!(
        (
            r["status"].clone(),
            r["resumed_by_user_id"].clone(),
            r["resumed_by_name"].clone()
        ),
        (
            json!("daily_inspection_pending"),
            Value::Null,
            json!("席 管理者")
        )
    );
    let c = send_h(&app, "POST", &u(it, "cancel"), t, &key, reason())
        .await
        .json();
    assert_eq!(c["status"], "cancelled");

    // 開発用の席の鍵: どの点呼にも触れる (前の版で残った dev 軸の遠隔点呼を一覧から取り消す)
    let dev = [("is_dev", "true")];
    let remote = sess(t, emp, "pre_operation", "遠隔点呼", "medical_pending", &dev).await;
    let c = send_h(&app, "POST", &u(remote, "cancel"), t, &dev_key, reason())
        .await
        .json();
    assert_eq!(c["status"], "cancelled");
    let auto = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &dev).await;
    let i = send_h(&app, "POST", &u(auto, "interrupt"), t, &dev_key, reason())
        .await
        .json();
    assert_eq!(i["status"], "interrupted");
    let r = send_h(&app, "POST", &u(auto, "resume"), t, &dev_key, reason())
        .await
        .json();
    assert_eq!(r["status"], "daily_inspection_pending");
}

/// 業務前 八: 体温・血圧の測定の有効時間 (社員の基準値。無ければ 30 分)。過ぎた値は受けず、再開・自己再開・
/// アルコールの段の直前へ進める段は、アルコール未測定で過ぎていれば体温・血圧の測り直しへ戻す
#[tokio::test]
async fn medical_validity_window() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "有効 一郎").await;
    // 要求の今は 2026-10-22T22:00:00Z。30 分前より古い値は過ぎている
    let old = "'2026-10-22T21:00:00Z'";
    let fresh = "'2026-10-22T21:50:00Z'";
    let medical = |at: &str| json!({ "temperature": 36.5, "systolic": 120, "diastolic": 80, "medical_measured_at": at });

    // PUT …/medical: 既定の 30 分
    let m = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let uri = format!("/api/tenko/sessions/{m}/medical");
    let got = send(&app, "PUT", &uri, t, Some(medical("2026-10-22T21:00:00Z"))).await;
    assert_eq!(got.status, 400);
    assert_eq!(
        got.json(),
        json!({ "error": "measurement_expired", "message": "測定から有効時間 (30 分) を過ぎています。もう一度測定してください" })
    );
    assert_eq!(session_status(m).await, "medical_pending");
    let ok = send(&app, "PUT", &uri, t, Some(medical("2026-10-22T21:40:00Z"))).await;
    assert_eq!(
        (ok.status, ok.json()["status"].clone()),
        (200, json!("self_declaration_pending"))
    );

    // 社員の基準値の有効時間 (90 分) が使われる
    let emp90 = employee(t, "有効 二郎").await;
    baseline(t, emp90).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.employee_health_baselines SET measurement_validity_minutes = 90 WHERE employee_id = $1",
            &[&emp90],
        )
        .await
        .unwrap();
    let m90 = sess(
        t,
        emp90,
        "pre_operation",
        "自動点呼",
        "medical_pending",
        &[],
    )
    .await;
    let uri90 = format!("/api/tenko/sessions/{m90}/medical");
    let got = send(
        &app,
        "PUT",
        &uri90,
        t,
        Some(medical("2026-10-22T20:00:00Z")),
    )
    .await;
    assert_eq!(got.status, 400);
    assert!(got.json()["message"].as_str().unwrap().contains("(90 分)"));
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri90,
            t,
            Some(medical("2026-10-22T21:00:00Z"))
        )
        .await
        .status,
        200
    );

    // 管理者の再開: 未測定で過ぎていれば測り直し。新しければ従来の再開先。アルコール測定済みなら戻さない
    let resume = |id: Uuid| format!("/api/tenko/sessions/{id}/resume");
    let body = Some(json!({ "reason": "確認済み" }));
    for (extra, want) in [
        (("medical_measured_at", old), "medical_pending"),
        (("medical_measured_at", fresh), "daily_inspection_pending"),
    ] {
        let id = sess(t, emp, "pre_operation", "自動点呼", "interrupted", &[extra]).await;
        let r = send_h(&app, "POST", &resume(id), t, &admin_headers(), body.clone()).await;
        assert_eq!(r.json()["status"], want);
        assert_eq!(session_status(id).await, want);
    }
    let measured = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "interrupted",
        &[("medical_measured_at", old), ("result", "'pass'")],
    )
    .await;
    let r = send_h(&app, "POST", &resume(measured), t, &admin_headers(), body).await;
    assert_eq!(r.json()["status"], "daily_inspection_pending");

    // キオスクの自己再開: 状態はそのまま、ただし過ぎていれば測り直し (測り直し中の再送は何もしない)
    let selfr = |id: Uuid| format!("/api/tenko/sessions/{id}/self-resume");
    let body = Some(json!({ "reason": "電池切れ" }));
    for (status, extra, want) in [
        (
            "daily_inspection_pending",
            ("medical_measured_at", old),
            "medical_pending",
        ),
        (
            "medical_pending",
            ("medical_measured_at", old),
            "medical_pending",
        ),
        (
            "daily_inspection_pending",
            ("medical_measured_at", fresh),
            "daily_inspection_pending",
        ),
    ] {
        let id = sess(t, emp, "pre_operation", "自動点呼", status, &[extra]).await;
        let r = send(&app, "POST", &selfr(id), t, body.clone()).await;
        assert_eq!(r.json()["status"], want);
    }

    // 携行品の確認の後 (アルコールの段の直前)
    let carry = |id: Uuid| format!("/api/tenko/sessions/{id}/carrying-items");
    for (extra, want) in [
        (("medical_measured_at", old), "medical_pending"),
        (("medical_measured_at", fresh), "identity_verified"),
    ] {
        let id = sess(
            t,
            emp,
            "pre_operation",
            "自動点呼",
            "carrying_items_pending",
            &[extra],
        )
        .await;
        let r = send(&app, "PUT", &carry(id), t, Some(json!({ "checks": [] }))).await;
        assert_eq!(r.json()["status"], want);
    }

    // 携行品が無いテナントでは日常点検の後が直前。携行品の確認が残るときはまだ見ない
    let none = tenant().await;
    let e2 = employee(none, "有効 三郎").await;
    let all_ok = json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    for (extra, want) in [
        (("medical_measured_at", old), "medical_pending"),
        (("medical_measured_at", fresh), "identity_verified"),
    ] {
        let id = sess(
            none,
            e2,
            "pre_operation",
            "自動点呼",
            "daily_inspection_pending",
            &[extra],
        )
        .await;
        let r = send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{id}/daily-inspection"),
            none,
            Some(all_ok.clone()),
        )
        .await;
        assert_eq!(r.json()["status"], want);
    }
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.carrying_items (tenant_id, item_name) VALUES ($1, '免許証')",
            &[&none],
        )
        .await
        .unwrap();
    let id = sess(
        none,
        e2,
        "pre_operation",
        "自動点呼",
        "daily_inspection_pending",
        &[("medical_measured_at", old)],
    )
    .await;
    let r = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{id}/daily-inspection"),
        none,
        Some(all_ok),
    )
    .await;
    assert_eq!(r.json()["status"], "carrying_items_pending");
}

/// 業務前 二十 ヨ・タ と 十三: 再開した運行管理者の氏名、代替措置、管理者が再開済みのセッションの自己再開
#[tokio::test]
async fn resume_name_alternative_measure_and_self_resume_after_manager() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "再開 一郎").await;
    let key = [("X-Device-Role", "device-tenko-manager")];
    let resume = |id: Uuid| format!("/api/tenko/sessions/{id}/resume");

    // 運行管理者の鍵からは氏名が必須 (空白だけも 400)。ログインの利用者は省略時にその利用者の表示 (名前が無ければメール)
    let id = sess(t, emp, "pre_operation", "IT点呼", "interrupted", &[]).await;
    for b in [
        json!({ "reason": "確認" }),
        json!({ "reason": "確認", "resumed_by_name": "  " }),
    ] {
        let r = send_h(&app, "POST", &resume(id), t, &key, Some(b)).await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (400, json!("resumed_by_name_required"))
        );
    }
    assert_eq!(session_status(id).await, "interrupted");
    let r = send_h(
        &app,
        "POST",
        &resume(id),
        t,
        &admin_headers(),
        Some(json!({ "reason": "確認" })),
    )
    .await
    .json();
    assert_eq!(r["resumed_by_name"], "admin@example.invalid");
    let id2 = sess(t, emp, "pre_operation", "IT点呼", "interrupted", &[]).await;
    let r = send_h(
        &app,
        "POST",
        &resume(id2),
        t,
        &admin_headers(),
        Some(json!({ "reason": "確認", "resumed_by_name": " 運行 太郎 " })),
    )
    .await
    .json();
    assert_eq!(r["resumed_by_name"], "運行 太郎");

    // キオスクの自己再開は管理者が再開済みでも受け、再開の時刻・理由・管理者を上書きしない
    let before = admin()
        .await
        .query_one(
            "SELECT resumed_at::text, resume_reason, resumed_by_user_id FROM alc_api.tenko_sessions WHERE id = $1",
            &[&id2],
        )
        .await
        .unwrap();
    let got = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id2}/self-resume"),
        t,
        Some(json!({ "reason": "キオスクから" })),
    )
    .await;
    assert_eq!(got.status, 200);
    let j = got.json();
    assert_eq!(
        (
            j["resume_reason"].clone(),
            j["resumed_by_user_id"].clone(),
            j["resumed_by_name"].clone(),
            j["status"].clone()
        ),
        (
            json!("確認"),
            json!(before.get::<_, Option<Uuid>>(2).map(|u| u.to_string())),
            json!("運行 太郎"),
            json!("daily_inspection_pending")
        )
    );
    let after = admin()
        .await
        .query_one(
            "SELECT resumed_at::text FROM alc_api.tenko_sessions WHERE id = $1",
            &[&id2],
        )
        .await
        .unwrap();
    assert_eq!(after.get::<_, String>(0), before.get::<_, String>(0));

    // 代替措置: 取り消しの本文に付き、記録 (record_data) にも入る。無ければ NULL
    let c1 = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let c = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{c1}/cancel"),
        t,
        Some(json!({ "reason": "体調不良", "alternative_measure": " 別の乗務員に交代 " })),
    )
    .await
    .json();
    assert_eq!(c["alternative_measure"], "別の乗務員に交代");
    assert_eq!(
        records(c1).await[0].2["alternative_measure"],
        "別の乗務員に交代"
    );
    let c2 = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let c = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{c2}/cancel"),
        t,
        Some(json!({ "reason": "体調不良" })),
    )
    .await
    .json();
    assert_eq!(c["alternative_measure"], Value::Null);

    // 判定: 従事できない (ng) 判定の代替措置は判定待ちを閉じるときにも、記録済みへの押し直しにも入る。ok では保存しない
    let judge = manager(t, "manager").await;
    let j = |id: Uuid| format!("/api/tenko/sessions/{id}/judgment");
    let jb = |v: &str, alt: Option<&str>| json!({ "judgment": v, "judged_by_employee_id": judge, "reason": "確認", "alternative_measure": alt });
    let pending = sess(t, emp, "pre_operation", "遠隔点呼", "judgment_pending", &[]).await;
    let r = send_h(
        &app,
        "POST",
        &j(pending),
        t,
        &admin_headers(),
        Some(jb("ng", Some("運行を見合わせる"))),
    )
    .await
    .json();
    assert_eq!(r["alternative_measure"], "運行を見合わせる");
    assert_eq!(
        records(pending).await[0].2["alternative_measure"],
        "運行を見合わせる"
    );
    let ok_pending = sess(t, emp, "pre_operation", "遠隔点呼", "judgment_pending", &[]).await;
    let r = send_h(
        &app,
        "POST",
        &j(ok_pending),
        t,
        &admin_headers(),
        Some(jb("ok", Some("不要"))),
    )
    .await
    .json();
    assert_eq!(r["alternative_measure"], Value::Null);
    let done = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    let r = send_h(
        &app,
        "POST",
        &j(done),
        t,
        &admin_headers(),
        Some(jb("ng", Some("交代"))),
    )
    .await
    .json();
    assert_eq!(r["alternative_measure"], "交代");
}

/// 業務前 二十 ハ: 車両番号。開始で予定の値を写し、`PUT …/vehicle` は車検証の登録番号 > 予定の値 > 手入力
#[tokio::test]
async fn vehicle_number_sources() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "車両 一郎").await;
    bp_history(t, emp, 10).await;

    // 開始: 予定に車両番号があれば写して source = schedule。無ければ NULL
    let with = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_schedules SET vehicle_number = ' 品川 100 あ 12-34 ' WHERE id = $1",
            &[&with],
        )
        .await
        .unwrap();
    let started = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": with, "employee_id": emp })),
    )
    .await
    .json();
    assert_eq!(
        (
            started["vehicle_number"].clone(),
            started["vehicle_number_source"].clone()
        ),
        (json!("品川 100 あ 12-34"), json!("schedule"))
    );
    let without = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let plain = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": without, "employee_id": emp })),
    )
    .await
    .json();
    assert_eq!(plain["vehicle_number"], Value::Null);
    let sid = started["id"].as_str().unwrap().to_owned();
    let uri = format!("/api/tenko/sessions/{sid}/vehicle");

    // どちらも無い / 空白だけ → 400
    for b in [
        json!({}),
        json!({ "carins_cert_no": " ", "vehicle_number": "" }),
    ] {
        let r = send(&app, "PUT", &uri, t, Some(b)).await;
        assert_eq!(
            (r.status, r.json()["error"].clone()),
            (400, json!("vehicle_required"))
        );
    }
    // 車検証に一致しない番号だけ → 400 (入力の車両番号が無い)
    let r = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "carins_cert_no": "999999999999" })),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (400, json!("vehicle_required"))
    );
    // 予定と同じ値 → schedule、違う値 → manual
    let j = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "vehicle_number": "品川 100 あ 12-34" })),
    )
    .await
    .json();
    assert_eq!(j["vehicle_number_source"], "schedule");
    let j = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "vehicle_number": "横浜 200 い 56-78" })),
    )
    .await
    .json();
    assert_eq!(
        (
            j["vehicle_number"].clone(),
            j["vehicle_number_source"].clone()
        ),
        (json!("横浜 200 い 56-78"), json!("manual"))
    );
    // 予定の無い点呼の手入力は manual
    let remote = sess(t, emp, "pre_operation", "遠隔点呼", "medical_pending", &[]).await;
    let ruri = format!("/api/tenko/sessions/{remote}/vehicle");
    let j = send(
        &app,
        "PUT",
        &ruri,
        t,
        Some(json!({ "vehicle_number": "札幌 300 う 90-12" })),
    )
    .await
    .json();
    assert_eq!(j["vehicle_number_source"], "manual");

    // 車検証に一致 → 登録番号 (EntryNoCarNo。CarNo は車台番号)・carins の列も埋める。入力の車両番号があっても車検証が優先
    support::car_inspection(t, "000000000077", "TESTCARID00077", "301231", "01").await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.car_inspection SET \"EntryNoCarNo\" = '品川 500 え 11-11', \"CarNo\" = 'ABC12-345678' WHERE tenant_id = $1 AND \"ElectCertMgNo\" = '000000000077'",
            &[&t],
        )
        .await
        .unwrap();
    let j = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "carins_cert_no": "000000000077", "vehicle_number": "無視される" })),
    )
    .await
    .json();
    assert_eq!(
        (
            j["vehicle_number"].clone(),
            j["vehicle_number_source"].clone(),
            j["carins_cert_no"].clone(),
            j["carins_matched_by"].clone(),
            j["carins_expires_on"].clone()
        ),
        (
            json!("品川 500 え 11-11"),
            json!("carins"),
            json!("000000000077"),
            json!("cert_no"),
            json!("2030-12-31")
        )
    );
    // 別のテナントの車検証は一致しない (入力の車両番号に落ちる)
    let other = tenant().await;
    let oe = employee(other, "車両 二郎").await;
    let os = sess(
        other,
        oe,
        "pre_operation",
        "自動点呼",
        "medical_pending",
        &[],
    )
    .await;
    let j = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{os}/vehicle"),
        other,
        Some(json!({ "carins_cert_no": "000000000077", "vehicle_number": "手入力" })),
    )
    .await
    .json();
    assert_eq!(
        (
            j["vehicle_number_source"].clone(),
            j["carins_cert_no"].clone()
        ),
        (json!("manual"), Value::Null)
    );

    // 終わった点呼・無い点呼
    let done = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    let r = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{done}/vehicle"),
        t,
        Some(json!({ "vehicle_number": "x" })),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (400, json!("session_not_editable"))
    );
    let gone = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{}/vehicle", Uuid::new_v4()),
        t,
        Some(json!({ "vehicle_number": "x" })),
    )
    .await;
    assert_eq!(
        (gone.status, gone.json()),
        (404, json!({ "error": "session_not_found" }))
    );
}

/// 業務前の自動点呼は血圧の記録が 10 日分そろっているときだけ。足りなければ予定は消費して遠隔点呼へ切り替える
/// (escalate-remote と同じ列)。dev 端末・業務後・予定なしは切り替えない。同じ日の複数件は 1 日
#[tokio::test]
async fn auto_pre_operation_needs_ten_days_of_bp() {
    let app = app().await;
    let t = tenant().await;
    let start = |sch: Uuid, emp: Uuid, h: Vec<(&'static str, &'static str)>| {
        let app = &app;
        async move {
            send_h(
                app,
                "POST",
                "/api/tenko/sessions/start",
                t,
                &h,
                Some(json!({ "schedule_id": sch, "employee_id": emp })),
            )
            .await
        }
    };
    let consumed = |sch: Uuid| async move {
        admin()
            .await
            .query_one(
                "SELECT consumed FROM alc_api.tenko_schedules WHERE id = $1",
                &[&sch],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    };

    // 9 日分: 201 で対面点呼 (通常点呼。遠隔点呼にはしないので escalated_to_remote_at は空) に切り替わり、予定は消費される。状態は medical_pending のまま
    let emp = employee(t, "血圧 九日").await;
    bp_history(t, emp, 9).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let got = start(sch, emp, vec![]).await;
    assert_eq!(got.status, 201);
    let s = got.json();
    assert_eq!(
        (
            s["tenko_method"].clone(),
            s["status"].clone(),
            s["remote_escalation_reason"].clone(),
            s["escalated_to_remote_at"].is_null(),
        ),
        (
            json!("通常点呼"),
            json!("medical_pending"),
            json!("血圧の記録が 10 日分に満たない (いま 9 日分) ため対面点呼"),
            true
        )
    );
    assert!(consumed(sch).await);

    // 10 日分: 自動点呼のまま
    let emp = employee(t, "血圧 十日").await;
    bp_history(t, emp, 10).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let s = start(sch, emp, vec![]).await.json();
    assert_eq!(
        (
            s["tenko_method"].clone(),
            s["remote_escalation_reason"].clone()
        ),
        (json!("自動点呼"), Value::Null)
    );

    // 同じ日の重複は 1 日: 9 日 + 同じ日の 5 件 = 9 日分
    let emp = employee(t, "血圧 重複").await;
    bp_history(t, emp, 9).await;
    bp_history(t, emp, 1).await;
    bp_history(t, emp, 1).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let s = start(sch, emp, vec![]).await.json();
    assert_eq!(s["tenko_method"], json!("通常点呼"));
    assert_eq!(
        s["remote_escalation_reason"],
        json!("血圧の記録が 10 日分に満たない (いま 9 日分) ため対面点呼")
    );

    // dev 端末は 0 日分でも自動点呼のまま (予定は dev の軸に置く。本番の軸の予定は dev の接続から見えない)
    let emp = employee(t, "血圧 開発").await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_schedules SET is_dev = TRUE WHERE id = $1",
            &[&sch],
        )
        .await
        .unwrap();
    let got = start(sch, emp, vec![("X-Device-Dev", "1")]).await;
    assert_eq!(got.status, 201);
    assert_eq!(got.json()["tenko_method"], json!("自動点呼"));

    // 業務後の予定 (0 日分) は自動点呼のまま、予定なし (遠隔点呼) も影響しない
    let emp = employee(t, "血圧 業務後").await;
    let sch = schedule(t, emp, "post_operation", None).await;
    let s = start(sch, emp, vec![]).await.json();
    assert_eq!(
        (
            s["tenko_method"].clone(),
            s["remote_escalation_reason"].clone()
        ),
        (json!("自動点呼"), Value::Null)
    );
    let remote = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "employee_id": emp, "tenko_type": "pre_operation" })),
    )
    .await;
    assert_eq!(remote.status, 201);
    assert_eq!(remote.json()["remote_escalation_reason"], Value::Null);
}

/// 血圧が 9 日分で対面点呼へ切り替わった (予定つきの) セッションを、自動点呼と同じ段で判定待ちまで通す。
/// 医療 → 自己申告 → 日常点検 → (携行品なし) → アルコール → 指示の確認で判定待ち。(セッション id, 判定の本文の部品) を返す
async fn walk_switched_to_pending(app: &App, t: Uuid, name: &str) -> String {
    let emp = employee(t, name).await;
    baseline(t, emp).await;
    bp_history(t, emp, 9).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let s = send(
        app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await
    .json();
    assert_eq!(s["tenko_method"], "通常点呼");
    assert_eq!(s["schedule_id"], json!(sch));
    let id = s["id"].as_str().unwrap().to_owned();
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");

    let med = send(
        app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6, "systolic": 125, "diastolic": 82, "pulse": 70 })),
    )
    .await
    .json();
    assert_eq!(med["status"], "self_declaration_pending");
    let decl = send(
        app,
        "PUT",
        &uri("self-declaration"),
        t,
        Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
    )
    .await
    .json();
    assert_eq!(decl["status"], "daily_inspection_pending");
    assert_eq!(decl["safety_judgment"]["status"], "pass");
    let ok = json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    // 携行品が無いテナントなので、日常点検の後はアルコールの段へ進む
    let insp = send(app, "PUT", &uri("daily-inspection"), t, Some(ok)).await;
    assert_eq!(insp.json()["status"], "identity_verified");
    let alc = measure(app, t, emp, &id, "normal").await;
    assert_eq!(alc["status"], "instruction_pending");
    let done = send(app, "PUT", &uri("instruction-confirm"), t, None).await;
    assert_eq!(done.json()["status"], "judgment_pending");
    assert!(records(id.parse().unwrap()).await.is_empty());
    id
}

/// 切り替えた対面点呼の判定: 同じ営業所 (既定) の運行管理者なら通常点呼のまま完了して記録を書く。
/// 確認の方法は対面に決まり (`it` や不明な範囲は 400)、乗務員は判定待ちを取り消せない
#[tokio::test]
async fn switched_in_person_session_walks_to_judgment() {
    let app = app().await;
    let t = tenant().await;
    let judge = manager(t, "manager").await;
    let id = walk_switched_to_pending(&app, t, "切替 一郎").await;
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");

    let cancel = send(
        &app,
        "POST",
        &uri("cancel"),
        t,
        Some(json!({ "reason": "やめる" })),
    )
    .await;
    assert_eq!(cancel.status, 400);
    let it = send_h(
        &app,
        "POST",
        &uri("judgment"),
        t,
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": judge, "method": "it" })),
    )
    .await;
    assert_eq!(it.status, 400);

    // 車両も運行管理者も所属が未設定 → 決められない (同じ営業所として通常点呼で記録する)
    let scope = send_h(
        &app,
        "GET",
        &format!("{}?judge={judge}", uri("in-person-scope")),
        t,
        &admin_headers(),
        None,
    )
    .await;
    assert_eq!(scope.status, 200);
    assert_eq!(
        scope.json(),
        json!({ "scope": "unknown", "vehicle_number": null, "vehicle_offices": [], "vehicle_source": null, "judge_offices": [] })
    );

    let judged = send_h(
        &app,
        "POST",
        &uri("judgment"),
        t,
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": judge, "reason": "確認" })),
    )
    .await;
    assert_eq!(judged.status, 200);
    let j = judged.json();
    assert_eq!(
        (
            j["status"].clone(),
            j["tenko_method"].clone(),
            j["manager_judgment_method"].clone()
        ),
        (json!("completed"), json!("通常点呼"), json!("in_person"))
    );
    let recs = records(id.parse().unwrap()).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(
        (recs[0].0.as_str(), recs[0].1.as_str()),
        ("completed", "通常点呼")
    );
}

/// 点呼の営業所を 1 つ作り、車両 (登録番号) と運行管理者を所属させる
async fn office(
    t: Uuid,
    name: &str,
    company: Option<&str>,
    vehicles: &[&str],
    managers: &[Uuid],
) -> Uuid {
    let db = admin().await;
    let id: Uuid = db
        .query_one(
            "INSERT INTO alc_api.tenko_offices (tenant_id, name, company_name) VALUES ($1, $2, $3) RETURNING id",
            &[&t, &name, &company],
        )
        .await
        .unwrap()
        .get(0);
    for v in vehicles {
        db.execute(
            "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, office_id) VALUES ($1, $2, $3)",
            &[&t, v, &id],
        )
        .await
        .unwrap();
    }
    for m in managers {
        db.execute(
            "INSERT INTO alc_api.tenko_office_managers (tenant_id, office_id, employee_id) VALUES ($1, $2, $3)",
            &[&t, &id, m],
        )
        .await
        .unwrap();
    }
    id
}

/// セッションの車両番号を書く (`PUT …/vehicle` の代わり)
async fn set_vehicle(id: &str, number: &str) {
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_sessions SET vehicle_number = $1 WHERE id = $2",
            &[&number, &id.parse::<Uuid>().unwrap()],
        )
        .await
        .unwrap();
}

/// 車両と同じ営業所の運行管理者 (兼務を含む) が判定したら通常点呼のまま。車両番号の空白の違いは照合で無視する。
/// 判定の前に見せる口 (`in-person-scope`) の権限・切り替えていないセッション・無いセッション
#[tokio::test]
async fn switched_in_person_same_office_by_affiliation() {
    let app = app().await;
    let t = tenant().await;
    let judge = manager(t, "manager").await;
    office(t, "本社営業所", None, &["品川100あ1234"], &[judge]).await;
    office(t, "別の営業所", None, &[], &[judge]).await;
    let id = walk_switched_to_pending(&app, t, "切替 三郎").await;
    set_vehicle(&id, "品川 100 あ　1234").await;
    let scope_uri = |s: &str| format!("/api/tenko/sessions/{s}/in-person-scope?judge={judge}");

    let scope = send_h(&app, "GET", &scope_uri(&id), t, &admin_headers(), None).await;
    assert_eq!(scope.status, 200);
    let j = scope.json();
    assert_eq!(j["scope"], "same_office");
    assert_eq!(j["vehicle_number"], "品川 100 あ　1234");
    assert_eq!(j["vehicle_offices"][0]["name"], "本社営業所");
    assert_eq!(j["vehicle_source"], "manual");
    assert_eq!(j["judge_offices"].as_array().unwrap().len(), 2);
    // 運行管理者の鍵でも見られる。キオスクの鍵・鍵なしは 401
    let key = [("X-Device-Role", "device-tenko-manager")];
    assert_eq!(
        send_h(&app, "GET", &scope_uri(&id), t, &key, None)
            .await
            .status,
        200
    );
    assert_eq!(
        send(&app, "GET", &scope_uri(&id), t, None).await.status,
        401
    );
    // 無いセッションは 404、切り替えていないセッションは 400
    assert_eq!(
        send_h(
            &app,
            "GET",
            &scope_uri(&Uuid::new_v4().to_string()),
            t,
            &admin_headers(),
            None
        )
        .await
        .status,
        404
    );
    let emp = employee(t, "切替 なし").await;
    let plain = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    assert_eq!(
        send_h(
            &app,
            "GET",
            &scope_uri(&plain.to_string()),
            t,
            &admin_headers(),
            None
        )
        .await
        .status,
        400
    );

    let judged = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id}/judgment"),
        t,
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": judge })),
    )
    .await;
    assert_eq!(judged.status, 200);
    assert_eq!(judged.json()["tenko_method"], "通常点呼");
}

/// 車検証を 1 枚作り、使用の本拠の位置・使用者・登録番号を書く
async fn car_inspection_base(
    t: Uuid,
    cert_no: &str,
    car_no: &str,
    use_base: &str,
    user_name: &str,
) {
    support::car_inspection(t, cert_no, &format!("CAR-{cert_no}"), "301231", "01").await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.car_inspection SET \"EntryNoCarNo\" = $1, \"UseheadqrterChar\" = $2, \"UsernameHighLevelChar\" = $3 \
             WHERE tenant_id = $4 AND \"ElectCertMgNo\" = $5",
            &[&car_no, &use_base, &user_name, &t, &cert_no],
        )
        .await
        .unwrap();
}

/// 車検証の使用の本拠・使用者の組を営業所に対応づける
async fn carins_base(t: Uuid, office: Uuid, use_base: &str, user_name: &str) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_office_carins_bases (tenant_id, office_id, use_base, user_name) VALUES ($1, $2, $3, $4)",
            &[&t, &office, &use_base, &user_name],
        )
        .await
        .unwrap();
}

/// 手で選んだ所属が無ければ、車両の車検証 (登録番号で一致する最新の 1 枚) の使用の本拠・使用者の組から営業所を決める。
/// 手で選んだ所属があればそちらが優先する。別のテナントの車検証・対応は見えない
#[tokio::test]
async fn switched_in_person_vehicle_office_from_carins() {
    let app = app().await;
    let t = tenant().await;
    let judge = manager(t, "manager").await;
    let nagasaki = office(t, "長崎営業所", None, &[], &[judge]).await;
    let saga = office(t, "佐賀営業所", None, &[], &[]).await;
    car_inspection_base(
        t,
        "CERT-CARINS-1",
        "佐賀 100 あ 7601",
        " 長崎県長崎市 ",
        "一歩運輸",
    )
    .await;
    carins_base(t, nagasaki, "長崎県長崎市", "一歩運輸").await;
    // 別のテナントの同じ登録番号の車検証は見えない
    let other = tenant().await;
    car_inspection_base(
        other,
        "CERT-CARINS-2",
        "佐賀100あ7601",
        "佐賀県佐賀市",
        "一歩運輸",
    )
    .await;
    let id = walk_switched_to_pending(&app, t, "切替 四郎").await;
    set_vehicle(&id, "佐賀100あ7601").await;
    let scope_uri = format!("/api/tenko/sessions/{id}/in-person-scope?judge={judge}");

    let j = send_h(&app, "GET", &scope_uri, t, &admin_headers(), None)
        .await
        .json();
    assert_eq!(
        (
            j["scope"].clone(),
            j["vehicle_source"].clone(),
            j["vehicle_offices"][0]["name"].clone()
        ),
        (json!("same_office"), json!("carins"), json!("長崎営業所"))
    );

    // 手で別の営業所に所属させると、車検証より優先する → 別の営業所 = 遠隔点呼とみなす
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, office_id) VALUES ($1, '佐賀 100 あ 7601', $2)",
            &[&t, &saga],
        )
        .await
        .unwrap();
    let j = send_h(&app, "GET", &scope_uri, t, &admin_headers(), None)
        .await
        .json();
    assert_eq!(
        (
            j["scope"].clone(),
            j["vehicle_source"].clone(),
            j["vehicle_offices"][0]["name"].clone()
        ),
        (json!("other_office"), json!("manual"), json!("佐賀営業所"))
    );
    let judged = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id}/judgment"),
        t,
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": judge })),
    )
    .await;
    assert_eq!(judged.json()["tenko_method"], "遠隔点呼");
}

/// 車両の営業所と運行管理者の所属営業所が違う (別の営業所・グループ企業) ときは、告示 266 号 第四条 2 項により
/// 遠隔点呼とみなし、判定で点呼方法を遠隔点呼に書き換えて記録する。画面が送る古い `in_person_scope` は見ない
#[tokio::test]
async fn switched_in_person_other_office_is_recorded_as_remote() {
    let app = app().await;
    let t = tenant().await;
    let judge = manager(t, "manager").await;
    office(t, "帯広営業所", None, &["帯広100か5678"], &[]).await;
    office(t, "グループ営業所", Some("グループ運輸"), &[], &[judge]).await;
    let id = walk_switched_to_pending(&app, t, "切替 二郎").await;
    set_vehicle(&id, "帯広100か5678").await;
    let scope = send_h(
        &app,
        "GET",
        &format!("/api/tenko/sessions/{id}/in-person-scope?judge={judge}"),
        t,
        &admin_headers(),
        None,
    )
    .await
    .json();
    assert_eq!(scope["scope"], "other_office");
    assert_eq!(scope["judge_offices"][0]["company_name"], "グループ運輸");
    let judged = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id}/judgment"),
        t,
        &admin_headers(),
        Some(json!({
            "judgment": "ok",
            "judged_by_employee_id": judge,
            "method": "in_person",
            "in_person_scope": "same_office"
        })),
    )
    .await;
    assert_eq!(judged.status, 200);
    let j = judged.json();
    assert_eq!(
        (
            j["status"].clone(),
            j["tenko_method"].clone(),
            j["manager_judgment_method"].clone()
        ),
        (json!("completed"), json!("遠隔点呼"), json!("in_person"))
    );
    let recs = records(id.parse().unwrap()).await;
    assert_eq!(recs.len(), 1);
    assert_eq!(
        (recs[0].0.as_str(), recs[0].1.as_str()),
        ("completed", "遠隔点呼")
    );
}

/// キオスクの車両番号の候補 (登録番号の一連指定番号の先頭で照合。車台番号 CarNo では出さない): 打った数字で始まる車両を、車検証 (車両 ID ごとに最新の 1 枚) と車両マスタから引き、
/// 営業所への紐づけ (手の所属、無ければ車検証の使用の本拠・使用者の対応) を付ける。端末の営業所の車両が先、
/// 次に打った数字で終わる車両。全角の数字・空白の違いは無視する。別のテナントの車両は出ない
#[tokio::test]
async fn vehicle_candidates_for_kiosk() {
    let app = app().await;
    let t = tenant().await;
    let emp = employee(t, "候補 一郎").await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[],
    )
    .await;
    let db = admin().await;
    let office = |name: &'static str| {
        let db = &db;
        async move {
            db.query_one(
                "INSERT INTO alc_api.tenko_offices (tenant_id, name) VALUES ($1, $2) RETURNING id",
                &[&t, &name],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0)
        }
    };
    let saga = office("佐賀営業所").await;
    let nagasaki = office("長崎営業所").await;
    support::car_inspection(t, "CERT-CAND-1", "CAR-CAND-1", "301231", "01").await;
    db.execute(
        "UPDATE alc_api.car_inspection SET \"EntryNoCarNo\" = '佐賀　１００　あ　７６０１', \"CarNo\" = 'BKR66E7601001', \"UseheadqrterChar\" = '＊＊＊', \
             \"UsernameHighLevelChar\" = '一歩運輸' WHERE tenant_id = $1 AND \"ElectCertMgNo\" = 'CERT-CAND-1'",
        &[&t],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO alc_api.tenko_office_carins_bases (tenant_id, office_id, use_base, user_name) VALUES ($1, $2, '＊＊＊', '一歩運輸')",
        &[&t, &saga],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, office_id) VALUES \
             ($1, '長崎 100 か 7601', $2), ($1, '福岡 100 さ 1760', NULL), ($1, '熊本 100 た 1234', NULL)",
        &[&t, &nagasaki],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO alc_api.tenko_office_devices (tenant_id, device_id, office_id) VALUES ($1, 'kiosk-nagasaki', $2)",
        &[&t, &nagasaki],
    )
    .await
    .unwrap();
    let other = tenant().await;
    db.execute(
        "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number) VALUES ($1, '他社 100 あ 7601')",
        &[&other],
    )
    .await
    .unwrap();
    let uri = |q: &str| format!("/api/tenko/sessions/{id}/vehicle-candidates?q={q}");
    let numbers = |v: &Value| -> Vec<String> {
        v["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["vehicle_number"].as_str().unwrap().to_owned())
            .collect()
    };

    // 端末の営業所 (長崎) の車両が先。次に 7601 で終わる車両、最後に途中に含む車両
    let device = [("X-Device-Id", "kiosk-nagasaki")];
    let j = send_h(&app, "GET", &uri("7601"), t, &device, None)
        .await
        .json();
    assert_eq!(
        numbers(&j),
        ["長崎 100 か 7601", "佐賀　１００　あ　７６０１"]
    );
    assert_eq!(
        (
            j["candidates"][0]["office_name"].clone(),
            j["candidates"][0]["linked"].clone(),
            j["candidates"][0]["device_office"].clone(),
            j["candidates"][0]["carins_cert_no"].clone()
        ),
        (json!("長崎営業所"), json!(true), json!(true), json!(null))
    );
    assert_eq!(
        (
            j["candidates"][1]["office_name"].clone(),
            j["candidates"][1]["device_office"].clone(),
            j["candidates"][1]["carins_cert_no"].clone()
        ),
        (json!("佐賀営業所"), json!(false), json!("CERT-CAND-1"))
    );
    // 端末の名乗りが無ければ営業所で並べない。紐づいていない車両は linked が false
    let j = send(&app, "GET", &uri("76"), t, None).await.json();
    let mut got = numbers(&j);
    got.sort();
    let mut want = vec!["佐賀　１００　あ　７６０１", "長崎 100 か 7601"];
    want.sort();
    assert_eq!(got, want);
    assert!(j["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["device_office"] == false && c["linked"] == true));
    // 一連指定番号の先頭で照合する (分類番号の 100 や途中の数字では出さない)。紐づいていない車両は linked が false
    let j = send(&app, "GET", &uri("17"), t, None).await.json();
    assert_eq!(numbers(&j), ["福岡 100 さ 1760"]);
    assert_eq!(j["candidates"][0]["linked"], false);
    assert_eq!(
        send(&app, "GET", &uri("100"), t, None).await.json(),
        json!({ "candidates": [] })
    );
    // 車台番号 (CarNo) の数字では出さない
    assert_eq!(
        send(&app, "GET", &uri("66"), t, None).await.json(),
        json!({ "candidates": [] })
    );
    // 当たらなければ空、数字でなければ 400、無いセッションは 404
    assert_eq!(
        send(&app, "GET", &uri("999"), t, None).await.json(),
        json!({ "candidates": [] })
    );
    for bad in ["", "7a", "123456789"] {
        assert_eq!(send(&app, "GET", &uri(bad), t, None).await.status, 400);
    }
    assert_eq!(
        send(
            &app,
            "GET",
            &format!(
                "/api/tenko/sessions/{}/vehicle-candidates?q=1",
                Uuid::new_v4()
            ),
            t,
            None
        )
        .await
        .status,
        404
    );
}
