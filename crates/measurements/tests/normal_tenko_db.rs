//! 通常点呼 (運行者端末の測定) が点呼セッション・点呼記録として残ることを実 DB で固定する
//! (rust-alc-api の tests/normal_tenko_record_test.rs の 26 件の写し。Refs ippoan/alc-app#238, ippoan/alc-app-s3#135)。
//!
//! **実 DB でしか検証できない** — 冪等の担保は migration 141 の部分 unique と `ON CONFLICT DO NOTHING` に任せている。
//!
//! rust のテストは、記録を作った後に rust に残る点呼の口 (`/api/tenko/records`・`/csv`・`/api/tenko/sessions`・
//! `/judgment`・`/api/car-inspections/lookup`・`/api/tenko/sessions/start`・`/alcohol`) でも結果を見ている。
//! それらの口はこの Worker に無いので、**その口が読む行 (点呼セッション・点呼記録の列と record_data) を DB で直接**確かめる。
//! 判定・一覧・CSV そのものの振る舞いは rust のテストが持つ。

mod support;

use serde_json::Value;
use support::*;
use uuid::Uuid;

#[tokio::test]
async fn test_normal_measurement_creates_tenko_session_and_record() {
    // 印つきの測定で session と record が 1 件ずつできる
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT1").await;

    let m = post_measurement(&app, tenant, emp, "normal", "2026-09-12T01:00:00Z", true).await;
    let m_id = m["id"].as_str().unwrap();
    assert_eq!(m["status"], "completed");

    let sessions = sessions_for_measurement(tenant, m_id).await;
    assert_eq!(sessions.len(), 1, "通常点呼のセッションが 1 件できる");
    assert_eq!(sessions[0].0, "normal");
    assert_eq!(sessions[0].1, "completed");
    assert_eq!(sessions[0].2, None);
    assert_eq!(
        type_and_method(tenant, m_id).await,
        (
            vec![("normal".to_string(), "通常点呼".to_string())],
            vec![("normal".to_string(), "通常点呼".to_string())],
        ),
        "tenko_type 無しは normal、session にも点呼方法 '通常点呼'"
    );

    // 点呼記録: 点呼方法 '通常点呼' / 執行者は空欄 / 体温・血圧が写っている
    // (rust はこの後 /api/tenko/records・/csv・/api/tenko/sessions でも 1 件出ることを見る。その口は rust に在る)
    let rec = admin()
        .await
        .query_one(
            "SELECT tenko_method, responsible_manager_name, temperature, systolic, employee_name,
                    record_hash, record_data->>'id', session_id
             FROM alc_api.tenko_records WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap();
    assert_eq!(rec.get::<_, String>(0), "通常点呼");
    assert_eq!(rec.get::<_, Option<String>>(1), None, "点呼執行者は空欄");
    assert_eq!(rec.get::<_, Option<f64>>(2), Some(36.5));
    assert_eq!(rec.get::<_, Option<i32>>(3), Some(120));
    assert_eq!(rec.get::<_, String>(4), "運行者");
    assert_eq!(
        rec.get::<_, String>(5).len(),
        64,
        "record_hash は SHA-256 の hex"
    );
    assert_eq!(
        rec.get::<_, Option<String>>(6),
        Some(rec.get::<_, Uuid>(7).to_string()),
        "record_data はセッションの JSON"
    );
}

#[tokio::test]
async fn test_normal_measurement_is_idempotent() {
    // 完了 PUT の 2 回目・同時刻の測定の 2 件目でも記録は 1 組
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT2").await;
    let measured_at = "2026-09-12T02:00:00Z";

    let m = post_measurement(&app, tenant, emp, "normal", measured_at, true).await;
    let m_id = m["id"].as_str().unwrap().to_string();
    assert_eq!(counts(tenant).await, (1, 1));

    // 同じ測定への完了 PUT を 2 回 (オンラインの再送)
    for _ in 0..2 {
        let body = serde_json::json!({ "status": "completed", "record_as_tenko": true });
        let res = put_measurement(&app, tenant, &m_id, &body).await;
        assert_eq!(res.status, 200);
    }
    assert_eq!(
        counts(tenant).await,
        (1, 1),
        "同じ測定への再送で記録は増えない"
    );

    // オンラインの完了 PUT が落ちて offline 保存へ回り、同じ乗務員・同じ測定時刻の測定がもう 1 件できる経路
    let m2 = post_measurement(&app, tenant, emp, "normal", measured_at, true).await;
    assert_ne!(
        m2["id"].as_str().unwrap(),
        m_id,
        "測定自体は 2 件目ができる"
    );
    assert_eq!(
        counts(tenant).await,
        (1, 1),
        "同じ乗務員・同じ測定時刻なら記録は 1 組のまま"
    );
}

#[tokio::test]
async fn test_normal_measurement_over_is_cancelled() {
    // アルコール検知 (over) はセッションが中止になる
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT3").await;

    let m = post_measurement(&app, tenant, emp, "over", "2026-09-12T03:00:00Z", true).await;
    let sessions = sessions_for_measurement(tenant, m["id"].as_str().unwrap()).await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].1, "cancelled");
    assert_eq!(sessions[0].2.as_deref(), Some("アルコール検知"));
    assert_eq!(counts(tenant).await, (1, 1));
}

#[tokio::test]
async fn test_normal_measurement_error_result_is_not_recorded() {
    // 結果が error の測定は保存だけされ、記録は作らない
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT4").await;

    let m = post_measurement(&app, tenant, emp, "error", "2026-09-12T04:00:00Z", true).await;
    assert_eq!(m["status"], "completed");
    assert_eq!(counts(tenant).await, (0, 0));
}

#[tokio::test]
async fn test_measurement_without_mark_creates_nothing() {
    // 印なしの測定は今までどおり記録を作らない
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT5").await;

    post_measurement(&app, tenant, emp, "normal", "2026-09-12T05:00:00Z", false).await;
    assert_eq!(counts(tenant).await, (0, 0));
}

/// 点呼記録の INSERT を**この tenant の行に限って**失敗させる trigger を張る (並行して走る他のテストには当たらない)
async fn fail_record_insert(tenant: Uuid) {
    let su = admin().await;
    su.batch_execute(
        r#"CREATE OR REPLACE FUNCTION alc_api.fail_tenko_record_insert_for_test()
           RETURNS TRIGGER AS $$
           BEGIN
               RAISE EXCEPTION 'injected failure for test';
           END;
           $$ LANGUAGE plpgsql"#,
    )
    .await
    .unwrap();
    su.batch_execute(&format!(
        "CREATE TRIGGER trg_fail_tenko_record_insert_{}
         BEFORE INSERT ON alc_api.tenko_records
         FOR EACH ROW WHEN (NEW.tenant_id = '{tenant}')
         EXECUTE FUNCTION alc_api.fail_tenko_record_insert_for_test()",
        tenant.simple()
    ))
    .await
    .unwrap();
}

async fn drop_fail_record_insert(tenant: Uuid) {
    admin()
        .await
        .batch_execute(&format!(
            "DROP TRIGGER trg_fail_tenko_record_insert_{} ON alc_api.tenko_records",
            tenant.simple()
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_measurement_survives_failed_record_creation() {
    // 記録の作成が失敗しても測定は残る (内側だけ rollback)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT6").await;
    fail_record_insert(tenant).await;

    let m = post_measurement(&app, tenant, emp, "normal", "2026-09-12T06:00:00Z", true).await;

    // PUT も同じ: 測定は更新され、応答の tenko_session_id は null
    let m_id = start_measurement(&app, tenant, emp).await;
    let body = completed_body("normal", "2026-09-12T06:30:00Z", true, None, None);
    let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
    assert_eq!(json["status"], "completed");
    assert!(json["tenko_session_id"].is_null());
    drop_fail_record_insert(tenant).await;

    // 測定は commit されている
    let res = call(
        &app,
        "GET",
        &format!("/api/measurements/{}", m["id"].as_str().unwrap()),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 200, "記録の作成が失敗しても測定は残る");
    assert_eq!(measurement_status(&m_id).await, "completed");

    // 記録側は内側の rollback でセッションごと巻き戻る
    assert_eq!(counts(tenant).await, (0, 0));
    // ログは固定の語だけ (SQLSTATE P0001 = RAISE EXCEPTION。例外の文・識別子を出さない)
    let logs = app.logs();
    assert_eq!(
        logs,
        vec![
            "measurements: normal tenko record failed: body (P0001)".to_string(),
            "measurements: normal tenko record failed: body (P0001)".to_string(),
        ]
    );
}

/// 始業 / 終業を選んだ測定が、その種別の点呼セッション・点呼記録になることを確かめる
async fn assert_typed_record(tenko_type: &str, code: &str, measured_at: &str) {
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", code).await;

    let res = send_measurement(
        &app,
        tenant,
        emp,
        "normal",
        measured_at,
        true,
        Some(tenko_type),
    )
    .await;
    assert_eq!(res.status, 201);
    let m = res.json();

    let expected = vec![(tenko_type.to_string(), "通常点呼".to_string())];
    assert_eq!(
        type_and_method(tenant, m["id"].as_str().unwrap()).await,
        (expected.clone(), expected),
        "session・record とも tenko_type={tenko_type}、点呼方法は '通常点呼'"
    );
    assert_eq!(counts(tenant).await, (1, 1));
}

#[tokio::test]
async fn test_normal_measurement_with_pre_operation() {
    assert_typed_record("pre_operation", "NT7", "2026-09-12T07:00:00Z").await;
}

#[tokio::test]
async fn test_normal_measurement_with_post_operation() {
    assert_typed_record("post_operation", "NT8", "2026-09-12T08:00:00Z").await;
}

#[tokio::test]
async fn test_invalid_tenko_type_is_rejected() {
    // 不正な種別は 400 で、測定も記録も作らない (POST / PUT)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT9").await;

    let res = send_measurement(
        &app,
        tenant,
        emp,
        "normal",
        "2026-09-12T09:00:00Z",
        true,
        Some("mid_operation"),
    )
    .await;
    assert_eq!(res.status, 400);
    assert_eq!(measurement_count(tenant).await, 0);
    assert_eq!(counts(tenant).await, (0, 0));

    let m_id = start_measurement(&app, tenant, emp).await;
    let mut body = completed_body("normal", "2026-09-12T09:30:00Z", true, None, None);
    body["tenko_type"] = Value::from("pre");
    let res = put_measurement(&app, tenant, &m_id, &body).await;
    assert_eq!(res.status, 400);
    assert_eq!(
        measurement_status(&m_id).await,
        "started",
        "400 の PUT は測定を更新しない"
    );
    assert_eq!(counts(tenant).await, (0, 0));
}

#[tokio::test]
async fn test_typed_record_is_idempotent() {
    // 同じ測定へ pre の PUT 2 回・同じ乗務員同時刻の normal と post でも 1 組
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT10").await;

    let m = post_measurement(&app, tenant, emp, "normal", "2026-09-12T10:00:00Z", false).await;
    let m_id = m["id"].as_str().unwrap();
    for _ in 0..2 {
        let body = serde_json::json!({
            "status": "completed",
            "record_as_tenko": true,
            "tenko_type": "pre_operation",
        });
        assert_eq!(put_measurement(&app, tenant, m_id, &body).await.status, 200);
    }
    assert_eq!(counts(tenant).await, (1, 1));
    let pre = vec![("pre_operation".to_string(), "通常点呼".to_string())];
    assert_eq!(type_and_method(tenant, m_id).await, (pre.clone(), pre));

    // 同じ乗務員・同じ measured_at で normal → post
    let measured_at = "2026-09-12T11:00:00Z";
    post_measurement(&app, tenant, emp, "normal", measured_at, true).await;
    let res = send_measurement(
        &app,
        tenant,
        emp,
        "normal",
        measured_at,
        true,
        Some("post_operation"),
    )
    .await;
    assert_eq!(res.status, 201, "測定自体は保存される");
    assert_eq!(
        counts(tenant).await,
        (2, 2),
        "同じ乗務員・同じ測定時刻の normal と post は 1 組 (先の 1 組と合わせて 2 組)"
    );
}

#[tokio::test]
async fn test_old_version_row_still_blocks_typed_resend() {
    // 切り替えの窓: 旧版が作った (normal, DEFAULT) の session があれば pre の再送でも 1 組
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT11").await;

    let m = post_measurement(&app, tenant, emp, "normal", "2026-09-12T12:00:00Z", false).await;
    let m_id = m["id"].as_str().unwrap();

    // 旧版の INSERT を模す: tenko_method を指定しない (DEFAULT '自動点呼' が入る)
    let started: chrono::DateTime<chrono::Utc> = "2026-09-12T12:00:00Z".parse().unwrap();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_sessions (
                 tenant_id, employee_id, tenko_type, status, measurement_id,
                 started_at, completed_at
             ) VALUES ($1, $2, 'normal', 'completed', $3, $4, NOW())",
            &[&tenant, &emp, &Uuid::parse_str(m_id).unwrap(), &started],
        )
        .await
        .unwrap();

    let body = serde_json::json!({
        "status": "completed",
        "record_as_tenko": true,
        "tenko_type": "pre_operation",
    });
    let res = put_measurement(&app, tenant, m_id, &body).await;
    assert_eq!(res.status, 200, "再送の保存は成功する");

    let (sessions, records) = type_and_method(tenant, m_id).await;
    assert_eq!(
        sessions,
        vec![("normal".to_string(), "自動点呼".to_string())],
        "旧版の 1 行のまま (pre の 2 行目はできない)"
    );
    assert!(records.is_empty(), "記録も増えない");
    // 旧版の行は tenko_type = 'normal' なので、応答の id はその行
    assert_eq!(
        res.json()["tenko_session_id"],
        session_rows(tenant, m_id).await[0].0.to_string()
    );
}

#[tokio::test]
async fn test_remote_tenko_session_is_not_caught_by_normal_flow_index() {
    // スケジュール無し (遠隔点呼) の session は '通常点呼' ではないため、通常点呼と同じ測定を付けても unique に当たらない
    // (Refs ippoan/rust-alc-api#655)。rust は遠隔点呼の口 (start → alcohol) で付ける。その口は rust に在るので、
    // ここでは遠隔点呼の session を DB に直接作ってから同じ測定を付ける
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT12").await;

    let res = send_measurement(
        &app,
        tenant,
        emp,
        "pass",
        "2026-09-12T13:00:00Z",
        true,
        Some("post_operation"),
    )
    .await;
    assert_eq!(res.status, 201);
    let m_id = Uuid::parse_str(res.json()["id"].as_str().unwrap()).unwrap();

    let su = admin().await;
    let remote: Uuid = su
        .query_one(
            "INSERT INTO alc_api.tenko_sessions (tenant_id, employee_id, tenko_type, status, tenko_method)
             VALUES ($1, $2, 'post_operation', 'identity_verified', '遠隔点呼') RETURNING id",
            &[&tenant, &emp],
        )
        .await
        .unwrap()
        .get(0);
    let updated = su
        .execute(
            "UPDATE alc_api.tenko_sessions SET measurement_id = $2, alcohol_result = 'pass', alcohol_value = 0
             WHERE id = $1",
            &[&remote, &m_id],
        )
        .await
        .expect("遠隔点呼の measurement_id の書き込みは部分 unique に当たらない");
    assert_eq!(updated, 1);

    // 測定の再送の応答に載る id は通常の流れの session (遠隔点呼の行は拾わない)
    let body = serde_json::json!({ "status": "completed", "record_as_tenko": true });
    let json = put_measurement_ok(&app, tenant, &m_id.to_string(), &body).await;
    assert_ne!(json["tenko_session_id"], remote.to_string());
    assert!(json["tenko_session_id"].is_string());
}

// ---------------------------------------------------------------------------
// 電子車検証の番号と carins の車検期限 (migration 142、Refs ippoan/alc-app-s3#110)。値はすべて合成値
// ---------------------------------------------------------------------------

/// 電子車検証の番号付きで通常点呼の測定を保存する
async fn send_carins_measurement(
    app: &App,
    tenant: Uuid,
    employee_id: Uuid,
    measured_at: &str,
    cert_no: Option<&str>,
    vehicle_id: Option<&str>,
) -> Got {
    let mut body = serde_json::json!({
        "employee_id": employee_id,
        "alcohol_value": 0.0,
        "result_type": "normal",
        "measured_at": measured_at,
        "record_as_tenko": true,
    });
    if let Some(v) = cert_no {
        body["carins_cert_no"] = Value::from(v);
    }
    if let Some(v) = vehicle_id {
        body["carins_vehicle_id"] = Value::from(v);
    }
    call(app, "POST", "/api/measurements", Some(tenant), Some(body)).await
}

#[tokio::test]
async fn test_carins_expiry_is_recorded() {
    // 管理番号一致 / 車両 ID 一致 / 未登録 / 番号なし を session・record_data に残す
    // (rust は CSV の末尾 4 列でも見る。CSV は rust に在り、record_data の同じ 4 値を読む)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NTC1").await;
    car_inspection(tenant, "000000000001", "TESTCARID00001", "301231", "01").await;

    let cases = [
        (
            "2026-09-15T01:00:00Z",
            Some("000000000001"),
            Some("TESTCARID00009"),
            cols(
                Some("000000000001"),
                Some("TESTCARID00009"),
                Some("2030-12-31"),
                Some("cert_no"),
            ),
        ),
        (
            "2026-09-15T02:00:00Z",
            Some("000000000009"),
            Some("TESTCARID00001"),
            cols(
                Some("000000000009"),
                Some("TESTCARID00001"),
                Some("2030-12-31"),
                Some("car_id"),
            ),
        ),
        // carins に無い → none・期限 NULL (点呼は記録する)
        (
            "2026-09-15T03:00:00Z",
            Some("000000000008"),
            None,
            cols(Some("000000000008"), None, None, Some("none")),
        ),
        // 番号を送らない (空文字も無しと同じ) → 4 列とも NULL
        (
            "2026-09-15T04:00:00Z",
            Some(""),
            None,
            cols(None, None, None, None),
        ),
    ];
    for (measured_at, cert_no, vehicle_id, expected) in cases {
        let res =
            send_carins_measurement(&app, tenant, emp, measured_at, cert_no, vehicle_id).await;
        assert_eq!(res.status, 201);
        assert_eq!(
            carins_of(tenant, &res.json()).await,
            (expected.clone(), expected)
        );
    }
    assert_eq!(counts(tenant).await, (4, 4));
}

#[tokio::test]
async fn test_carins_newer_row_of_same_car_wins() {
    // 同じ車両 ID で管理番号の違う古い行と新しい行 → 新しい期限
    // (rust は kiosk の照合口 /api/car-inspections/lookup でも同じ SQL を見る。その口は rust に在る)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NTC2").await;
    // 継続検査の前 (古い管理番号) と後 (新しい管理番号)
    car_inspection(tenant, "000000000002", "TESTCARID00002", "250101", "01").await;
    car_inspection(tenant, "000000000003", "TESTCARID00002", "301231", "02").await;

    // 古い電子車検証の番号でタップしても、同じ車の新しい期限が出る
    let res = send_carins_measurement(
        &app,
        tenant,
        emp,
        "2026-09-15T05:00:00Z",
        Some("000000000002"),
        Some("TESTCARID00002"),
    )
    .await;
    assert_eq!(res.status, 201);
    let (session, _) = carins_of(tenant, &res.json()).await;
    assert_eq!(session.2.as_deref(), Some("2030-12-31"));
    assert_eq!(
        session.3.as_deref(),
        Some("car_id"),
        "新しい行は管理番号が違うので car_id で一致"
    );

    // 別の tenant の同じ番号の行は引かない (RLS と WHERE の tenant_id)
    let other = support::tenant().await;
    let other_emp = employee(other, "運行者", "NTC2").await;
    let res = send_carins_measurement(
        &app,
        other,
        other_emp,
        "2026-09-15T05:00:00Z",
        Some("000000000003"),
        None,
    )
    .await;
    assert_eq!(res.status, 201);
    assert_eq!(
        carins_of(other, &res.json()).await.0,
        cols(Some("000000000003"), None, None, Some("none"))
    );
}

#[tokio::test]
async fn test_carins_broken_expiry_still_records() {
    // 壊れた期限の行でも記録は作られ、期限は NULL
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NTC3").await;
    // 月が 13 — 正規表現で外れて NULL (照合は成功)
    car_inspection(tenant, "000000000004", "TESTCARID00004", "301399", "01").await;
    // 2 月 31 日 — 正規表現は通るが to_date が失敗する (照合の SQL エラー)
    car_inspection(tenant, "000000000005", "TESTCARID00005", "300231", "01").await;

    let res = send_carins_measurement(
        &app,
        tenant,
        emp,
        "2026-09-15T06:00:00Z",
        Some("000000000004"),
        None,
    )
    .await;
    assert_eq!(res.status, 201);
    let expected = cols(Some("000000000004"), None, None, Some("cert_no"));
    assert_eq!(
        carins_of(tenant, &res.json()).await,
        (expected.clone(), expected)
    );

    let res = send_carins_measurement(
        &app,
        tenant,
        emp,
        "2026-09-15T07:00:00Z",
        Some("000000000005"),
        Some("TESTCARID00005"),
    )
    .await;
    assert_eq!(res.status, 201);
    let expected = cols(Some("000000000005"), Some("TESTCARID00005"), None, None);
    assert_eq!(
        carins_of(tenant, &res.json()).await,
        (expected.clone(), expected),
        "照合に失敗しても番号は保存し、期限と matched_by は NULL"
    );
    assert_eq!(counts(tenant).await, (2, 2));
    // ログは段と SQLSTATE だけ (to_date の範囲外。番号を出さない)
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: carins lookup failed: body (22")
            && !logs[0].contains("0000"),
        "{logs:?}"
    );
}

#[tokio::test]
async fn test_invalid_carins_number_is_rejected() {
    // 不正な桁の番号は 400 で、測定も記録も作らない (POST / PUT)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NTC4").await;

    let res = send_carins_measurement(
        &app,
        tenant,
        emp,
        "2026-09-15T08:00:00Z",
        Some("0000000001"),
        None,
    )
    .await;
    assert_eq!(res.status, 400);
    assert_eq!(measurement_count(tenant).await, 0);

    let m_id = start_measurement(&app, tenant, emp).await;
    let mut body = completed_body("normal", "2026-09-15T08:30:00Z", true, None, None);
    body["carins_vehicle_id"] = Value::from("TESTCARID-0001");
    let res = put_measurement(&app, tenant, &m_id, &body).await;
    assert_eq!(res.status, 400);
    assert_eq!(
        measurement_status(&m_id).await,
        "started",
        "400 の PUT は測定を更新しない"
    );
    assert_eq!(counts(tenant).await, (0, 0));
}

// =========================================================================
// 点呼方法 (通常点呼 / IT点呼) と、保存の応答に載る点呼セッションの id (Refs ippoan/alc-app#387)
// =========================================================================

#[tokio::test]
async fn test_put_without_tenko_method_stays_normal_and_returns_session_id() {
    // tenko_method を送らない PUT は通常点呼のまま、応答に session の id が載る
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT30").await;

    // 未指定 と 明示の '通常点呼' は同じ結果になる
    for (method, measured_at) in [
        (None, "2026-09-13T01:00:00Z"),
        (Some("通常点呼"), "2026-09-13T02:00:00Z"),
    ] {
        let m_id = start_measurement(&app, tenant, emp).await;
        let body = completed_body("normal", measured_at, true, None, method);
        let json = put_measurement_ok(&app, tenant, &m_id, &body).await;

        // 既存の欄はそのまま (入れ子にならない)
        assert_eq!(json["id"], m_id);
        assert_eq!(json["status"], "completed");
        assert_eq!(json["result_type"], "normal");
        assert_eq!(json["employee_id"], emp.to_string());
        assert_eq!(json["temperature"], 36.5);

        let rows = session_rows(tenant, &m_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            json["tenko_session_id"],
            rows[0].0.to_string(),
            "応答の id は作られた session の id (tenko_method={method:?})"
        );
        let normal = vec![("normal".to_string(), "通常点呼".to_string())];
        assert_eq!(
            type_and_method(tenant, &m_id).await,
            (normal.clone(), normal),
            "session も record も '通常点呼' (tenko_method={method:?})"
        );
    }
    assert_eq!(counts(tenant).await, (2, 2));
}

#[tokio::test]
async fn test_put_it_tenko_records_method_on_session_and_record() {
    // IT点呼 は判定待ち (judgment_pending) で作り、記録は運行管理者の判定で書く。検知 (中止) だけは判定を待たずに閉じて記録する
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT31").await;

    // (送る種別, 記録される種別, 結果, 期待する状態)
    let cases = [
        (None, "normal", "normal", "judgment_pending"),
        (
            Some("pre_operation"),
            "pre_operation",
            "pass",
            "judgment_pending",
        ),
        (
            Some("post_operation"),
            "post_operation",
            "normal",
            "judgment_pending",
        ),
        // アルコール検知は IT点呼 でも中止 (status_for_result を共有している)
        (Some("pre_operation"), "pre_operation", "over", "cancelled"),
    ];
    for (i, (tenko_type, recorded_type, result, status)) in cases.into_iter().enumerate() {
        let measured_at = format!("2026-09-13T1{i}:00:00Z");
        let m_id = start_measurement(&app, tenant, emp).await;
        let body = completed_body(result, &measured_at, true, tenko_type, Some("IT点呼"));
        let json = put_measurement_ok(&app, tenant, &m_id, &body).await;

        let rows = session_rows(tenant, &m_id).await;
        assert_eq!(rows.len(), 1, "session は 1 行 (tenko_type={tenko_type:?})");
        assert_eq!(json["tenko_session_id"], rows[0].0.to_string());
        assert_eq!(rows[0].1, recorded_type);
        assert_eq!(rows[0].2, "IT点呼");
        assert_eq!(rows[0].3, status, "結果 {result} → {status}");

        let expected = vec![(recorded_type.to_string(), "IT点呼".to_string())];
        let records = if status == "cancelled" {
            expected.clone()
        } else {
            vec![]
        };
        assert_eq!(
            type_and_method(tenant, &m_id).await,
            (expected, records),
            "session の点呼方法は 'IT点呼'。記録は中止のときだけ (tenko_type={tenko_type:?})"
        );
    }
    assert_eq!(counts_any_method(tenant).await, (4, 1));
    assert_eq!(
        counts(tenant).await,
        (0, 0),
        "'通常点呼' の行は 1 つもできていない"
    );

    // 判定はまだ無い
    let judged: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND manager_judgment IS NOT NULL",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(judged, 0);
}

#[tokio::test]
async fn test_put_resend_returns_same_session_id() {
    // 同じ測定の再送は session 1 行のまま、応答の id も 1 回目と同じ
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT32").await;
    let measured_at = "2026-09-13T03:00:00Z";

    let m_id = start_measurement(&app, tenant, emp).await;
    let mut body = completed_body(
        "normal",
        measured_at,
        true,
        Some("pre_operation"),
        Some("IT点呼"),
    );
    body["identity_method"] = Value::from("ic_card");

    let first = put_measurement_ok(&app, tenant, &m_id, &body).await;
    let first_id = first["tenko_session_id"].as_str().unwrap().to_string();
    assert_eq!(counts_any_method(tenant).await, (1, 0));
    // IT点呼 は判定待ちなので記録はまだ無い (本人確認の方法は session の列にだけ在る)
    let ic_card = (vec![Some("ic_card".to_string())], vec![]);
    assert_eq!(identity_methods(tenant, &m_id).await, ic_card);

    // 同じ内容で再送 (ON CONFLICT DO NOTHING は対象指定なしなので、行数と id の両方で確かめる)
    for _ in 0..2 {
        let again = put_measurement_ok(&app, tenant, &m_id, &body).await;
        assert_eq!(again["tenko_session_id"], first_id, "再送でも同じ id");
        assert_eq!(again["status"], "completed", "測定の status");
    }
    assert_eq!(
        counts_any_method(tenant).await,
        (1, 0),
        "再送で session も record も増えない"
    );
    let rows = session_rows(tenant, &m_id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.to_string(), first_id);

    // 点呼方法を変えて再送しても、最初の 1 行のまま (書き換えない)
    let as_normal = put_measurement_ok(
        &app,
        tenant,
        &m_id,
        &completed_body("normal", measured_at, true, Some("pre_operation"), None),
    )
    .await;
    assert_eq!(as_normal["tenko_session_id"], first_id);
    let it = vec![("pre_operation".to_string(), "IT点呼".to_string())];
    assert_eq!(
        type_and_method(tenant, &m_id).await,
        (it, vec![]),
        "先に記録された点呼方法のまま"
    );
    assert_eq!(counts_any_method(tenant).await, (1, 0));

    // 本人確認の方法も先勝ち: 送らない再送 (上) でも、別の値の再送でも書き換えない
    assert_eq!(
        identity_methods(tenant, &m_id).await,
        ic_card,
        "identity_method を送らない再送で NULL に戻らない"
    );
    let mut as_manual = body.clone();
    as_manual["identity_method"] = Value::from("manual");
    let again = put_measurement_ok(&app, tenant, &m_id, &as_manual).await;
    assert_eq!(again["tenko_session_id"], first_id);
    assert_eq!(
        identity_methods(tenant, &m_id).await,
        ic_card,
        "別の値の再送でも、先に記録された本人確認の方法のまま"
    );
    assert_eq!(counts_any_method(tenant).await, (1, 0));

    // 同じ乗務員・同じ測定時刻の別の測定 (offline 保存へ回った経路) は (employee_id, started_at) の unique が弾く。
    // その測定には session が紐づいていないので、応答の id は null
    let other = start_measurement(&app, tenant, emp).await;
    let json = put_measurement_ok(&app, tenant, &other, &body).await;
    assert_eq!(json["status"], "completed", "測定自体は保存される");
    assert!(json["tenko_session_id"].is_null());
    assert_eq!(counts_any_method(tenant).await, (1, 0));
}

/// session の 1 行の identity_method を DB で直接 (rust は `/api/tenko/sessions/{id}` の応答で見る。その口は rust に在る)
async fn session_identity_method(session_id: &str) -> Option<String> {
    admin()
        .await
        .query_one(
            "SELECT identity_method FROM alc_api.tenko_sessions WHERE id = $1",
            &[&Uuid::parse_str(session_id).unwrap()],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn test_put_identity_method_is_stored_on_session_and_record_data() {
    // identity_method は session の列と record_data に入り、省略は NULL
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT35").await;

    // 5 つの値。点呼方法 (通常点呼 / IT点呼) とは独立に書かれる
    let cases = [
        ("license", None),
        ("ic_card", Some("IT点呼")),
        ("remote_punch", None),
        ("nfc_card", Some("通常点呼")),
        ("manual", Some("IT点呼")),
    ];
    for (i, (identity, tenko_method)) in cases.into_iter().enumerate() {
        let measured_at = format!("2026-09-14T1{i}:00:00Z");
        let m_id = start_measurement(&app, tenant, emp).await;
        let mut body = completed_body("normal", &measured_at, true, None, tenko_method);
        body["identity_method"] = Value::from(identity);
        let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
        assert!(
            !json.as_object().unwrap().contains_key("identity_method"),
            "測定の応答の形は変えていない"
        );
        // IT点呼 は判定待ちで記録がまだ無い (判定で書く記録の record_data に入る)
        let is_it = tenko_method == Some("IT点呼");
        let in_record = if is_it {
            vec![]
        } else {
            vec![(true, Some(identity.to_string()))]
        };
        assert_eq!(
            identity_methods(tenant, &m_id).await,
            (vec![Some(identity.to_string())], in_record),
            "session の列と record_data の両方に入る (identity_method={identity})"
        );
        let method = tenko_method.unwrap_or("通常点呼").to_string();
        let expected = vec![("normal".to_string(), method)];
        let records = if is_it { vec![] } else { expected.clone() };
        assert_eq!(
            type_and_method(tenant, &m_id).await,
            (expected, records),
            "点呼方法は identity_method に引きずられない"
        );
        let session_id = json["tenko_session_id"].as_str().unwrap();
        assert_eq!(
            session_identity_method(session_id).await.as_deref(),
            Some(identity)
        );
    }

    // 省略 (既存の端末): 列は NULL、record_data には key が在って null
    let m_id = start_measurement(&app, tenant, emp).await;
    let body = completed_body("normal", "2026-09-14T20:00:00Z", true, None, None);
    let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
    let none = (vec![None], vec![(true, None)]);
    assert_eq!(identity_methods(tenant, &m_id).await, none);
    let session_id = json["tenko_session_id"].as_str().unwrap().to_string();
    assert_eq!(session_identity_method(&session_id).await, None);

    // 先勝ち: 省略で記録された後に値を付けて再送しても NULL のまま
    let mut with_value = body.clone();
    with_value["identity_method"] = Value::from("license");
    let again = put_measurement_ok(&app, tenant, &m_id, &with_value).await;
    assert_eq!(again["tenko_session_id"], session_id);
    assert_eq!(
        identity_methods(tenant, &m_id).await,
        none,
        "2 回目の保存は書かない"
    );
    // IT点呼 の 2 件は判定待ちで記録がまだ無い
    assert_eq!(counts_any_method(tenant).await, (6, 4));

    // 知らない値は 400 (測定も更新しない・session も作らない)
    let m_id = start_measurement(&app, tenant, emp).await;
    let mut bad = completed_body("normal", "2026-09-14T21:00:00Z", true, None, None);
    bad["identity_method"] = Value::from("face");
    let res = put_measurement(&app, tenant, &m_id, &bad).await;
    assert_eq!(res.status, 400);
    assert_eq!(
        measurement_status(&m_id).await,
        "started",
        "400 の PUT は測定を更新しない"
    );
    // IT点呼 の 2 件は判定待ちで記録がまだ無い
    assert_eq!(counts_any_method(tenant).await, (6, 4));
}

#[tokio::test]
async fn test_put_without_session_returns_null_session_id() {
    // 印なし・点呼にならない結果・不正な点呼方法では session を作らない
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT33").await;

    // record_as_tenko: false (点呼方法を付けていても作らない)
    let m_id = start_measurement(&app, tenant, emp).await;
    let body = completed_body(
        "normal",
        "2026-09-13T04:00:00Z",
        false,
        None,
        Some("IT点呼"),
    );
    let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
    assert_eq!(json["status"], "completed");
    assert!(json.as_object().unwrap().contains_key("tenko_session_id"));
    assert!(json["tenko_session_id"].is_null());

    // 結果が error (点呼にならない測定)
    let m_id = start_measurement(&app, tenant, emp).await;
    let body = completed_body("error", "2026-09-13T05:00:00Z", true, None, Some("IT点呼"));
    let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
    assert_eq!(json["status"], "completed", "測定は保存される");
    assert!(json["tenko_session_id"].is_null());

    // まだ完了していない保存 (status を送らない途中の PUT)
    let m_id = start_measurement(&app, tenant, emp).await;
    let body = serde_json::json!({ "temperature": 36.4, "record_as_tenko": true });
    let json = put_measurement_ok(&app, tenant, &m_id, &body).await;
    assert_eq!(json["status"], "started");
    assert!(json["tenko_session_id"].is_null());

    // 自動点呼・遠隔点呼はこの口から作らせない (400、測定も更新しない)
    for method in ["自動点呼", "遠隔点呼"] {
        let body = completed_body("normal", "2026-09-13T06:00:00Z", true, None, Some(method));
        let res = put_measurement(&app, tenant, &m_id, &body).await;
        assert_eq!(res.status, 400, "tenko_method={method} は 400");
    }
    assert_eq!(
        measurement_status(&m_id).await,
        "started",
        "400 の PUT は測定を更新しない"
    );
    assert_eq!(counts_any_method(tenant).await, (0, 0));
}

#[tokio::test]
async fn test_post_ignores_tenko_method() {
    // POST は tenko_method を受けず、通常点呼のまま
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT34").await;

    let body = serde_json::json!({
        "employee_id": emp,
        "alcohol_value": 0.0,
        "result_type": "normal",
        "measured_at": "2026-09-13T07:00:00Z",
        "record_as_tenko": true,
        "tenko_method": "IT点呼",
        "identity_method": "ic_card",
    });
    let res = call(&app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 201);
    let m = res.json();
    assert!(
        !m.as_object().unwrap().contains_key("tenko_session_id"),
        "POST の応答の形は変えていない"
    );
    let m_id = m["id"].as_str().unwrap();
    let normal = vec![("normal".to_string(), "通常点呼".to_string())];
    assert_eq!(
        type_and_method(tenant, m_id).await,
        (normal.clone(), normal)
    );
    // POST は本人確認の方法も受けない (記録なし = NULL)
    assert_eq!(
        identity_methods(tenant, m_id).await,
        (vec![None], vec![(true, None)])
    );
}

// =========================================================================
// IT点呼 の判定・未完了の一覧・CSV (rust の口) が前提にする行 (alc-migrations 157、Refs ippoan/alc-app#387)。
// 判定 (`/judgment`)・一覧 (`/api/tenko/sessions`)・CSV は rust に在る。ここでは測定の保存が作る行が、
// その口の前提 (点呼方法・完了の状態・判定の列が空・記録の点呼方法) を満たすことを確かめる
// =========================================================================

/// 測定を開始 → 完了 PUT で点呼のセッションを 1 件作り、session の id を返す (`tenko_method` が None なら通常点呼)
async fn tenko_session_via_put(
    app: &App,
    tenant: Uuid,
    employee_id: Uuid,
    measured_at: &str,
    tenko_method: Option<&str>,
) -> String {
    let m_id = start_measurement(app, tenant, employee_id).await;
    let body = completed_body(
        "normal",
        measured_at,
        true,
        Some("pre_operation"),
        tenko_method,
    );
    let json = put_measurement_ok(app, tenant, &m_id, &body).await;
    json["tenko_session_id"].as_str().unwrap().to_string()
}

/// session の (tenko_method, status, measurement_id, manager_judgment, manager_judgment_method)
async fn judgment_view(
    session_id: &str,
) -> (String, String, Option<Uuid>, Option<String>, Option<String>) {
    let r = admin()
        .await
        .query_one(
            "SELECT tenko_method, status, measurement_id, manager_judgment, manager_judgment_method
             FROM alc_api.tenko_sessions WHERE id = $1",
            &[&Uuid::parse_str(session_id).unwrap()],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4))
}

/// 判定を打てる運行管理者 (role に manager)
async fn judge(tenant: Uuid, code: &str) -> Uuid {
    let id = employee(tenant, "運行管理者", code).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET role = ARRAY['manager'] WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    id
}

/// 管理者ログインで判定を打つ
async fn post_judgment(app: &App, tenant: Uuid, session_id: &str, body: Value) -> Got {
    let bytes = serde_json::to_vec(&body).unwrap();
    call_raw(
        app,
        "POST",
        &format!("/api/tenko/sessions/{session_id}/judgment"),
        Some(tenant),
        &[
            ("content-type", "application/json"),
            ("X-User-ID", "0b9f5a52-6a1e-4c1b-9d2a-3f4e5d6c7b8a"),
            ("X-User-Email", "admin@example.invalid"),
            ("X-User-Role", "admin"),
        ],
        bytes,
    )
    .await
}

#[tokio::test]
async fn test_it_tenko_session_waits_for_manager_judgment() {
    // IT点呼 の session は保存の直後は判定待ちで、記録はまだ無い。判定で閉じて記録を書く (判定が record_data に入る)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT35").await;
    let manager = judge(tenant, "NT35M").await;

    let session_id =
        tenko_session_via_put(&app, tenant, emp, "2026-09-13T08:00:00Z", Some("IT点呼")).await;
    let (method, status, measurement_id, judgment, judgment_method) =
        judgment_view(&session_id).await;
    assert_eq!(method, "IT点呼");
    assert_eq!(status, "judgment_pending");
    assert!(measurement_id.is_some());
    assert_eq!((judgment, judgment_method), (None, None));
    assert_eq!(counts_any_method(tenant).await, (1, 0));

    let res = post_judgment(
        &app,
        tenant,
        &session_id,
        serde_json::json!({ "judgment": "ok", "judged_by_employee_id": manager, "method": "it" }),
    )
    .await;
    assert_eq!(res.status, 200);
    let after = judgment_view(&session_id).await;
    assert_eq!(after.0, "IT点呼");
    assert_eq!(after.1, "completed", "判定で閉じる");
    assert_eq!(
        (after.3.as_deref(), after.4.as_deref()),
        (Some("ok"), Some("it"))
    );
    let data: Value = admin()
        .await
        .query_one(
            "SELECT record_data FROM alc_api.tenko_records WHERE session_id = $1",
            &[&Uuid::parse_str(&session_id).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(data["manager_judgment"], "ok");
    assert_eq!(data["manager_judgment_method"], "it");
}

#[tokio::test]
async fn test_it_tenko_judgment_method_is_stored_and_kept() {
    // IT点呼 の行の判定の確認の方法は空で始まり (rust の判定の口が「最初の確定に method が要る」を判断する前提)、
    // 通常点呼の行は点呼方法が '通常点呼' (rust の口が method を付けると 400 にする前提)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT41").await;

    let s_in_person =
        tenko_session_via_put(&app, tenant, emp, "2026-09-14T08:00:00Z", Some("IT点呼")).await;
    let s_it =
        tenko_session_via_put(&app, tenant, emp, "2026-09-14T09:00:00Z", Some("IT点呼")).await;
    let s_normal = tenko_session_via_put(&app, tenant, emp, "2026-09-14T10:00:00Z", None).await;

    for s in [&s_in_person, &s_it] {
        let v = judgment_view(s).await;
        assert_eq!(v.0, "IT点呼");
        assert_eq!((v.3, v.4), (None, None));
    }
    let v = judgment_view(&s_normal).await;
    assert_eq!(v.0, "通常点呼");
    assert_eq!((v.3, v.4), (None, None));
}

#[tokio::test]
async fn test_list_pending_it_tenko_sessions() {
    // 未完了の IT点呼 の一覧 (rust の `/api/tenko/sessions?tenko_method=IT点呼&judgment_pending=true`) が拾う行:
    // 点呼方法 'IT点呼' かつ manager_judgment IS NULL。通常点呼と判定済みは入らない
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT42").await;

    let it_a =
        tenko_session_via_put(&app, tenant, emp, "2026-09-15T08:00:00Z", Some("IT点呼")).await;
    let it_b =
        tenko_session_via_put(&app, tenant, emp, "2026-09-15T09:00:00Z", Some("IT点呼")).await;
    let it_c =
        tenko_session_via_put(&app, tenant, emp, "2026-09-15T10:00:00Z", Some("IT点呼")).await;
    let normal = tenko_session_via_put(&app, tenant, emp, "2026-09-15T11:00:00Z", None).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_sessions SET manager_judgment = 'ok', manager_judgment_method = 'it'
             WHERE id = $1",
            &[&Uuid::parse_str(&it_c).unwrap()],
        )
        .await
        .unwrap();

    let pending: Vec<String> = admin()
        .await
        .query(
            "SELECT id::text FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND tenko_method = 'IT点呼' AND manager_judgment IS NULL
             ORDER BY id",
            &[&tenant],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let mut expected = vec![it_a, it_b];
    expected.sort();
    assert_eq!(pending, expected, "通常点呼と判定済みは入らない");
    assert_eq!(counts_any_method(tenant).await.0, 4);
    assert_eq!(judgment_view(&normal).await.0, "通常点呼");
}

#[tokio::test]
async fn test_csv_reads_in_person_it_tenko_as_in_person_tenko() {
    // 記録簿の CSV が読み替える元の行: 記録の tenko_method は保存のときの点呼方法のまま。
    // IT点呼 の記録は判定で書かれる ('IT点呼' 2 件と '通常点呼' 1 件。判定待ちの 1 件はまだ記録が無い)
    let app = app().await;
    let tenant = tenant().await;
    let emp = employee(tenant, "運行者", "NT43").await;

    let manager = judge(tenant, "NT43M").await;

    let s_in_person =
        tenko_session_via_put(&app, tenant, emp, "2026-09-16T08:00:00Z", Some("IT点呼")).await;
    let s_it =
        tenko_session_via_put(&app, tenant, emp, "2026-09-16T09:00:00Z", Some("IT点呼")).await;
    let _s_pending =
        tenko_session_via_put(&app, tenant, emp, "2026-09-16T10:00:00Z", Some("IT点呼")).await;
    let s_normal = tenko_session_via_put(&app, tenant, emp, "2026-09-16T11:00:00Z", None).await;
    for (sid, method) in [(&s_in_person, "in_person"), (&s_it, "it")] {
        let body = serde_json::json!({ "judgment": "ok", "judged_by_employee_id": manager, "method": method });
        assert_eq!(post_judgment(&app, tenant, sid, body).await.status, 200);
    }

    let records: Vec<(Uuid, String)> = admin()
        .await
        .query(
            "SELECT session_id, tenko_method FROM alc_api.tenko_records WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(records.len(), 3);
    let normal_sid = Uuid::parse_str(&s_normal).unwrap();
    for (session_id, tenko_method) in &records {
        let expected = if *session_id == normal_sid {
            "通常点呼"
        } else {
            "IT点呼"
        };
        assert_eq!(tenko_method, expected, "記録の行そのものは書き換わらない");
    }
}
