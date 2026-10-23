//! 血圧の通知の設定の 4 口 (`/api/bp-notify/...`。Refs ippoan/alc-app#436) を口から実 DB まで通す。LINE WORKS のメンバー一覧の
//! 送り先 (alc-lineworks-worker の Service Binding) は `MockService`。表は `bp_notify_settings` (alc-migrations 186)。
//! 4 口とも admin だけ。ログは固定の語と kind / status だけ (氏名・LINE WORKS の id・メールアドレスを出さない)。
//! 氏名・id・メールアドレスは全部架空。

mod support;

use std::sync::Arc;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const SETTINGS: &str = "/api/bp-notify/settings";
const USERS: &str = "/api/bp-notify/lineworks-users";
const LINK: &str = "/api/bp-notify/link-by-name";

fn app_with(service: &Arc<MockService>) -> impl std::future::Future<Output = App> {
    let service: measurements::service::SharedService = service.clone();
    async move { build_lineworks(rt_client().await, Some(service)) }
}

/// admin の呼び手として打つ (X-User-ID・X-User-Email・X-User-Role を揃える)
async fn as_role(
    app: &App,
    role: Option<&str>,
    method: &str,
    uri: &str,
    tenant: Uuid,
    body: Option<Value>,
) -> Got {
    let user = Uuid::new_v4().to_string();
    let mut headers: Vec<(&str, &str)> = match role {
        Some(r) => vec![
            ("X-User-ID", user.as_str()),
            ("X-User-Email", "admin@example.test"),
            ("X-User-Role", r),
        ],
        None => vec![],
    };
    let bytes = match &body {
        Some(v) => {
            headers.push(("content-type", "application/json"));
            serde_json::to_vec(v).unwrap()
        }
        None => Vec::new(),
    };
    call_raw(app, method, uri, Some(tenant), &headers, bytes).await
}

async fn send(app: &App, method: &str, uri: &str, tenant: Uuid, body: Option<Value>) -> Got {
    as_role(app, Some("admin"), method, uri, tenant, body).await
}

fn code() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_owned()
}

/// 社員を 1 人作る (superuser で)
async fn emp(tenant: Uuid, name: &str) -> Uuid {
    employee(tenant, name, &code()).await
}

/// 設定の行の中身 (superuser で。無ければ None)
async fn row(
    tenant: Uuid,
    emp: Uuid,
) -> Option<(Option<String>, Option<String>, Option<String>, bool, bool)> {
    admin()
        .await
        .query_opt(
            "SELECT lineworks_user_id, lineworks_name, email, notify_self, daily_report \
             FROM alc_api.bp_notify_settings WHERE tenant_id = $1 AND employee_id = $2",
            &[&tenant, &emp],
        )
        .await
        .unwrap()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
}

async fn count(tenant: Uuid) -> i64 {
    admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.bp_notify_settings WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0)
}

fn members(list: &[(&str, Option<&str>, Option<&str>)]) -> Value {
    Value::Array(
        list.iter()
            .map(|(id, name, email)| json!({"user_id": id, "user_name": name, "email": email}))
            .collect(),
    )
}

fn names(v: &Value, key: &str) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|e| e[key].as_str().unwrap().to_owned())
        .collect()
}

// ---- admin だけ ----

#[tokio::test]
async fn test_all_four_routes_are_admin_only() {
    let tenant = tenant().await;
    let e = emp(tenant, "山田太郎").await;
    let service = MockService::members(&members(&[]));
    let app = app_with(&service).await;
    let put = format!("{SETTINGS}/{e}");

    for (method, uri) in [
        ("GET", SETTINGS),
        ("PUT", put.as_str()),
        ("GET", USERS),
        ("POST", LINK),
    ] {
        // admin でない role・ヘッダ無し・role だけ (X-User-ID が無く AuthUser が復元されない) は 403
        let bare = [("X-User-Role", "admin")];
        for res in [
            as_role(&app, Some("viewer"), method, uri, tenant, None).await,
            as_role(&app, None, method, uri, tenant, None).await,
            call_raw(&app, method, uri, Some(tenant), &bare, Vec::new()).await,
        ] {
            assert_eq!(res.status, 403, "{method} {uri}");
            assert_eq!(res.json(), json!({"error": "forbidden"}));
        }
    }
    // 何も書かれず、LINE WORKS にも問い合わせていない
    assert_eq!(count(tenant).await, 0);
    assert!(service.gets().is_empty());
    assert!(app.logs().is_empty());
    // テナントのヘッダ無しは 401 (admin でも)
    let res = call_raw(&app, "GET", SETTINGS, None, &[], Vec::new()).await;
    assert_eq!(res.status, 401);
}

// ---- GET /settings ----

#[tokio::test]
async fn test_list_returns_every_live_employee_in_code_order_with_defaults() {
    let tenant = tenant().await;
    let other = tenant_with_employee().await;
    let late = employee(tenant, "後ろ", "b-002").await;
    let early = employee(tenant, "前", "a-001").await;
    let no_code: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.employees (id, tenant_id, nfc_id, name, code, role) \
             VALUES (gen_random_uuid(), $1, gen_random_uuid()::text, 'コード無し', NULL, ARRAY['driver']) RETURNING id",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    let gone = employee(tenant, "削除済み", "c-003").await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
            &[&gone],
        )
        .await
        .unwrap();
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET role = ARRAY['admin', 'manager'] WHERE id = $1",
            &[&early],
        )
        .await
        .unwrap();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bp_notify_settings \
                 (tenant_id, employee_id, lineworks_user_id, lineworks_name, email, notify_self, daily_report) \
             VALUES ($1, $2, 'lw-late', '後ろ表示', 'late@example.invalid', false, false)",
            &[&tenant, &late],
        )
        .await
        .unwrap();
    let app = app_with(&MockService::ok()).await;

    let res = send(&app, "GET", SETTINGS, tenant, None).await;

    assert_eq!(res.status, 200);
    let body = res.json();
    let list = body["employees"].as_array().unwrap();
    // 社員コード順 (コード無しは最後)。削除済み・別のテナントの社員は出ない
    assert_eq!(
        names(&body["employees"], "name"),
        ["前", "後ろ", "コード無し"]
    );
    assert_eq!(
        list[0],
        json!({
            "employee_id": early, "name": "前", "code": "a-001", "role": ["admin", "manager"],
            "lineworks_user_id": null, "lineworks_name": null, "email": null,
            "notify_self": true, "daily_report": true
        })
    );
    assert_eq!(
        list[1],
        json!({
            "employee_id": late, "name": "後ろ", "code": "b-002", "role": ["driver"],
            "lineworks_user_id": "lw-late", "lineworks_name": "後ろ表示", "email": "late@example.invalid",
            "notify_self": false, "daily_report": false
        })
    );
    assert_eq!(list[2]["employee_id"], json!(no_code));
    assert_eq!(list[2]["code"], Value::Null);
    assert_eq!(list[2]["role"], json!(["driver"]));
    let _ = other;
}

/// 別のテナントに社員が 1 人居る状態を作る (一覧に混ざらないことの確認用)
async fn tenant_with_employee() -> Uuid {
    let t = tenant().await;
    emp(t, "別のテナントの人").await;
    t
}

#[tokio::test]
async fn test_list_db_failure_is_a_500_with_a_fixed_log() {
    let tenant = tenant().await;
    let app = build_lineworks(broken_client().await, None);

    let res = send(&app, "GET", SETTINGS, tenant, None).await;

    assert_eq!(res.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: bp settings list failed: db ("),
        "{logs:?}"
    );
}

// ---- PUT /settings/{employee_id} ----

#[tokio::test]
async fn test_put_creates_then_updates_only_the_fields_that_came() {
    let tenant = tenant().await;
    let e = emp(tenant, "山田太郎").await;
    let app = app_with(&MockService::ok()).await;
    let uri = format!("{SETTINGS}/{e}");

    // 行が無い社員へ一部だけ: 来ていない欄は既定 (null / true)。値の前後の空白は除く
    let res = send(
        &app,
        "PUT",
        &uri,
        tenant,
        Some(json!({"lineworks_user_id": "  lw-1 ", "email": "yamada@example.invalid"})),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(
        res.json(),
        json!({
            "employee_id": e, "name": "山田太郎", "code": res.json()["code"], "role": res.json()["role"],
            "lineworks_user_id": "lw-1", "lineworks_name": null, "email": "yamada@example.invalid",
            "notify_self": true, "daily_report": true
        })
    );

    // 別の欄だけ: ほかはそのまま
    let res = send(
        &app,
        "PUT",
        &uri,
        tenant,
        Some(json!({"lineworks_name": "山田 太郎", "notify_self": false})),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(
        row(tenant, e).await,
        Some((
            Some("lw-1".into()),
            Some("山田 太郎".into()),
            Some("yamada@example.invalid".into()),
            false,
            true
        ))
    );
    assert_eq!(res.json()["notify_self"], false);

    // null は空にする。空文字・空白だけも空にする。真偽値の null は来ていないのと同じ
    let res = send(
        &app,
        "PUT",
        &uri,
        tenant,
        Some(
            json!({"lineworks_user_id": null, "lineworks_name": "   ", "email": null,
                    "notify_self": null, "daily_report": false}),
        ),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(row(tenant, e).await, Some((None, None, None, false, false)));

    // 本文なし: 行は作られたまま何も変わらない。別の社員の行は増えない
    let res = send(&app, "PUT", &uri, tenant, None).await;
    assert_eq!(res.status, 200);
    assert_eq!(row(tenant, e).await, Some((None, None, None, false, false)));
    assert_eq!(count(tenant).await, 1);
    // 成功のログは残さない
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_put_with_an_empty_body_creates_the_default_row() {
    let tenant = tenant().await;
    let e = emp(tenant, "山田太郎").await;
    let app = app_with(&MockService::ok()).await;

    let res = send(&app, "PUT", &format!("{SETTINGS}/{e}"), tenant, None).await;

    assert_eq!(res.status, 200);
    assert_eq!(row(tenant, e).await, Some((None, None, None, true, true)));
}

#[tokio::test]
async fn test_put_refuses_a_lineworks_user_already_used_by_another_employee() {
    let tenant = tenant().await;
    let (a, b) = (emp(tenant, "甲").await, emp(tenant, "乙").await);
    let other = tenant_with_employee().await;
    let other_emp = emp(other, "別のテナントの甲").await;
    let app = app_with(&MockService::ok()).await;
    let put = |who: Uuid, t: Uuid, id: &'static str| {
        let app = &app;
        async move {
            send(
                app,
                "PUT",
                &format!("{SETTINGS}/{who}"),
                t,
                Some(json!({"lineworks_user_id": id, "email": "x@example.invalid"})),
            )
            .await
        }
    };

    assert_eq!(put(a, tenant, "lw-shared").await.status, 200);
    // 同じテナントの別の社員 → 409。何も書かれない
    let res = put(b, tenant, "lw-shared").await;
    assert_eq!(res.status, 409);
    assert_eq!(res.json(), json!({"error": "lineworks_user_taken"}));
    assert_eq!(row(tenant, b).await, None);
    // 本人が同じ値で保存し直すのは通る。別のテナントの同じ id も通る (一意は tenant ごと)
    assert_eq!(put(a, tenant, "lw-shared").await.status, 200);
    assert_eq!(put(other_emp, other, "lw-shared").await.status, 200);
    // 空にした後は、別の社員が使える
    send(
        &app,
        "PUT",
        &format!("{SETTINGS}/{a}"),
        tenant,
        Some(json!({"lineworks_user_id": null})),
    )
    .await;
    assert_eq!(put(b, tenant, "lw-shared").await.status, 200);
    // 409 は想定した結果なのでログに出さない
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_put_unknown_deleted_or_other_tenants_employee_is_404() {
    let tenant = tenant().await;
    let gone = emp(tenant, "削除済み").await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
            &[&gone],
        )
        .await
        .unwrap();
    let other = support::tenant().await;
    let foreign = emp(other, "別のテナントの人").await;
    let app = app_with(&MockService::ok()).await;

    for who in [Uuid::new_v4(), gone, foreign] {
        let res = send(
            &app,
            "PUT",
            &format!("{SETTINGS}/{who}"),
            tenant,
            Some(json!({"email": "x@example.invalid"})),
        )
        .await;
        assert_eq!(res.status, 404, "{who}");
        assert_eq!(res.json(), json!({"error": "employee_not_found"}));
    }
    assert_eq!(count(tenant).await, 0);
    assert_eq!(count(other).await, 0);
}

#[tokio::test]
async fn test_put_rejects_a_bad_body_or_email() {
    let tenant = tenant().await;
    let e = emp(tenant, "山田太郎").await;
    let app = app_with(&MockService::ok()).await;
    let uri = format!("{SETTINGS}/{e}");

    for bad in [
        json!({"email": "no-at-sign"}),
        json!({"email": "a b@example.invalid"}),
        json!({"email": "a@example.invalid\nBcc: b@example.invalid"}),
    ] {
        let res = send(&app, "PUT", &uri, tenant, Some(bad)).await;
        assert_eq!(res.status, 400);
        assert_eq!(res.json(), json!({"error": "invalid_email"}));
    }
    // JSON でない・型が違う
    for bad in [
        &b"{"[..],
        &br#"{"notify_self": "yes"}"#[..],
        &b"[]"[..],
        &br#"{"email": 1}"#[..],
    ] {
        let res = call_raw(
            &app,
            "PUT",
            &uri,
            Some(tenant),
            &[
                ("X-User-ID", "00000000-0000-0000-0000-000000000001"),
                ("X-User-Email", "admin@example.test"),
                ("X-User-Role", "admin"),
                ("content-type", "application/json"),
            ],
            bad.to_vec(),
        )
        .await;
        assert_eq!(res.status, 400);
        assert_eq!(res.json(), json!({"error": "invalid_body"}));
    }
    // パスが UUID でない
    let res = send(&app, "PUT", &format!("{SETTINGS}/not-a-uuid"), tenant, None).await;
    assert_eq!(res.status, 400);
    assert_eq!(row(tenant, e).await, None);
}

#[tokio::test]
async fn test_put_db_failure_is_a_500_with_a_fixed_log() {
    let tenant = tenant().await;
    let e = emp(tenant, "山田太郎").await;
    let app = build_lineworks(broken_client().await, None);

    let res = send(&app, "PUT", &format!("{SETTINGS}/{e}"), tenant, None).await;

    assert_eq!(res.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: bp settings put failed: db ("),
        "{logs:?}"
    );
}

// ---- GET /lineworks-users ----

#[tokio::test]
async fn test_lineworks_users_returns_the_members() {
    let tenant = tenant().await;
    let service = MockService::members(&members(&[
        ("lw-1", Some("山田 太郎"), Some("yamada@example.invalid")),
        ("lw-2", None, None),
    ]));
    let app = app_with(&service).await;

    let res = send(&app, "GET", USERS, tenant, None).await;

    assert_eq!(res.status, 200);
    assert_eq!(
        res.json(),
        json!({"users": [
            {"user_id": "lw-1", "name": "山田 太郎", "email": "yamada@example.invalid"},
            {"user_id": "lw-2", "name": null, "email": null},
        ]})
    );
    let gets = service.gets();
    assert_eq!(gets.len(), 1);
    assert_eq!(gets[0].path, "/api/internal/lineworks/users");
    assert_eq!(gets[0].headers, [("X-Tenant-ID", tenant.to_string())]);
    assert!(service.requests().is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_lineworks_users_failures_are_502_or_503_with_a_fixed_message() {
    let tenant = tenant().await;
    let message = "LINE WORKS のメンバー一覧を取得できませんでした";
    for (service, status, log) in [
        (
            MockService::with_body(500, false, br#"{"error":"secret detail"}"#),
            502,
            "measurements: bp settings failed: lineworks (status 500)",
        ),
        (
            MockService::with(200, true),
            502,
            "measurements: bp settings failed: lineworks (fetch)",
        ),
        (
            MockService::with_body(200, false, b"not json"),
            502,
            "measurements: bp settings failed: lineworks (body)",
        ),
    ] {
        let app = app_with(&service).await;
        let res = send(&app, "GET", USERS, tenant, None).await;
        assert_eq!(res.status, status);
        assert_eq!(
            res.json(),
            json!({"error": "lineworks_unavailable", "message": message})
        );
        assert_eq!(app.logs(), [log]);
    }
    // binding が無い
    let app = build_lineworks(rt_client().await, None);
    let res = send(&app, "GET", USERS, tenant, None).await;
    assert_eq!(res.status, 503);
    assert_eq!(
        res.json(),
        json!({"error": "lineworks_unavailable", "message": message})
    );
    assert_eq!(
        app.logs(),
        ["measurements: bp settings failed: lineworks (no binding)"]
    );
}

// ---- POST /link-by-name ----

/// 山田・鈴木・佐藤・田中の 4 人のテナント。山田・鈴木は結び付けられる、佐藤は LINE WORKS に居ない、田中は 2 人居る
async fn link_setup() -> (Uuid, [Uuid; 4], Arc<MockService>) {
    let tenant = tenant().await;
    let ids = [
        employee(tenant, "山田 太郎", "01").await,
        employee(tenant, "鈴木　花子", "02").await,
        employee(tenant, "佐藤 次郎", "03").await,
        employee(tenant, "田中 一郎", "04").await,
    ];
    let service = MockService::members(&members(&[
        (
            "lw-yamada",
            Some("山田太郎"),
            Some("yamada@example.invalid"),
        ),
        ("lw-suzuki", Some("鈴木 花子"), None),
        ("lw-tanaka-1", Some("田中一郎"), None),
        ("lw-tanaka-2", Some("田中 一郎"), None),
        ("lw-other", Some("関係のない人"), None),
    ]));
    (tenant, ids, service)
}

#[tokio::test]
async fn test_link_dry_run_is_the_default_and_writes_nothing() {
    let (tenant, ids, service) = link_setup().await;
    let app = app_with(&service).await;

    for body in [None, Some(json!({})), Some(json!({"dry_run": true}))] {
        let res = send(&app, "POST", LINK, tenant, body).await;
        assert_eq!(res.status, 200);
        let v = res.json();
        assert_eq!(v["dry_run"], true);
        assert_eq!(v["linked"], 0);
        assert_eq!(
            v["candidates"],
            json!([
                {"employee_id": ids[0], "employee_name": "山田 太郎", "lineworks_user_id": "lw-yamada",
                 "lineworks_name": "山田太郎", "email": "yamada@example.invalid"},
                {"employee_id": ids[1], "employee_name": "鈴木　花子", "lineworks_user_id": "lw-suzuki",
                 "lineworks_name": "鈴木 花子", "email": null},
            ])
        );
        assert_eq!(
            v["skipped"],
            json!([
                {"employee_id": ids[2], "employee_name": "佐藤 次郎", "reason": "no_lineworks_user"},
                {"employee_id": ids[3], "employee_name": "田中 一郎", "reason": "multiple_lineworks_users"},
            ])
        );
    }
    assert_eq!(count(tenant).await, 0);
    assert_eq!(service.gets().len(), 3);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_link_confirm_writes_the_candidates_and_keeps_the_other_settings() {
    let (tenant, ids, service) = link_setup().await;
    // 鈴木は設定の行が既にある (本人への通知を切っている・自分で入れたメールがある)。メンバーに email が無ければメールは残す
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bp_notify_settings (tenant_id, employee_id, email, notify_self, daily_report) \
             VALUES ($1, $2, 'mine@example.invalid', false, false)",
            &[&tenant, &ids[1]],
        )
        .await
        .unwrap();
    let app = app_with(&service).await;

    let res = send(&app, "POST", LINK, tenant, Some(json!({"dry_run": false}))).await;

    assert_eq!(res.status, 200);
    let v = res.json();
    assert_eq!(v["dry_run"], false);
    assert_eq!(v["linked"], 2);
    assert_eq!(v["candidates"].as_array().unwrap().len(), 2);
    assert_eq!(v["skipped"].as_array().unwrap().len(), 2);
    assert_eq!(
        row(tenant, ids[0]).await,
        Some((
            Some("lw-yamada".into()),
            Some("山田太郎".into()),
            Some("yamada@example.invalid".into()),
            true,
            true
        ))
    );
    assert_eq!(
        row(tenant, ids[1]).await,
        Some((
            Some("lw-suzuki".into()),
            Some("鈴木 花子".into()),
            Some("mine@example.invalid".into()),
            false,
            false
        ))
    );
    assert_eq!(row(tenant, ids[2]).await, None);
    assert_eq!(row(tenant, ids[3]).await, None);

    // 続けてもう一度: 結び付いた社員は対象外。残りの 2 人が同じ理由で skipped
    let res = send(&app, "POST", LINK, tenant, Some(json!({"dry_run": false}))).await;
    let v = res.json();
    assert_eq!(v["linked"], 0);
    assert_eq!(v["candidates"], json!([]));
    assert_eq!(v["skipped"].as_array().unwrap().len(), 2);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_link_skips_a_member_already_tied_to_another_employee() {
    let tenant = tenant().await;
    let (a, b, deleted) = (
        emp(tenant, "山田 太郎").await,
        emp(tenant, "鈴木 花子").await,
        emp(tenant, "佐藤 次郎").await,
    );
    // 山田のメンバーは別の (名前の違う) 社員に、鈴木のメンバーは削除済みの社員に結び付いている
    let holder = emp(tenant, "別の人").await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.bp_notify_settings (tenant_id, employee_id, lineworks_user_id) \
             VALUES ($1, $2, 'lw-yamada'), ($1, $3, 'lw-suzuki')",
            &[&tenant, &holder, &deleted],
        )
        .await
        .unwrap();
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
            &[&deleted],
        )
        .await
        .unwrap();
    let service = MockService::members(&members(&[
        ("lw-yamada", Some("山田太郎"), None),
        ("lw-suzuki", Some("鈴木花子"), None),
    ]));
    let app = app_with(&service).await;

    let res = send(&app, "POST", LINK, tenant, Some(json!({"dry_run": false}))).await;

    assert_eq!(res.status, 200);
    let v = res.json();
    assert_eq!(v["linked"], 0);
    assert_eq!(v["candidates"], json!([]));
    let skipped: Vec<(Value, Value)> = v["skipped"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["employee_id"].clone(), s["reason"].clone()))
        .collect();
    assert!(skipped.contains(&(json!(a), json!("lineworks_user_taken"))));
    assert!(skipped.contains(&(json!(b), json!("lineworks_user_taken"))));
    // 結び付いている社員 (別の人) は対象外なので、skipped に出ない
    assert!(!skipped.iter().any(|(id, _)| *id == json!(holder)));
    assert_eq!(row(tenant, a).await, None);
}

#[tokio::test]
async fn test_link_loses_the_race_with_a_409_and_writes_nothing() {
    let (tenant, ids, service) = link_setup().await;
    // 2 人目の書き込みで一意索引の違反が起きる (読んだ後に他の人が先に結び付けた、の代わり)。1 人目の書きも巻き戻る
    let su = admin().await;
    let fname = format!("bp_link_race_{}", tenant.simple());
    su.batch_execute(&format!(
        "CREATE FUNCTION alc_api.{fname}() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'raced' USING ERRCODE = '23505'; END $$; \
         CREATE TRIGGER {fname} BEFORE INSERT OR UPDATE ON alc_api.bp_notify_settings \
         FOR EACH ROW WHEN (NEW.employee_id = '{}'::uuid) EXECUTE FUNCTION alc_api.{fname}()",
        ids[1]
    ))
    .await
    .unwrap();
    let app = app_with(&service).await;

    let res = send(&app, "POST", LINK, tenant, Some(json!({"dry_run": false}))).await;

    su.batch_execute(&format!(
        "DROP TRIGGER {fname} ON alc_api.bp_notify_settings; DROP FUNCTION alc_api.{fname}()"
    ))
    .await
    .unwrap();
    assert_eq!(res.status, 409);
    assert_eq!(res.json(), json!({"error": "lineworks_user_taken"}));
    assert_eq!(count(tenant).await, 0);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_link_never_touches_another_tenant() {
    let (tenant, _, service) = link_setup().await;
    let other = tenant_with_employee().await;
    let app = app_with(&service).await;

    send(&app, "POST", LINK, tenant, Some(json!({"dry_run": false}))).await;

    assert_eq!(count(other).await, 0);
    assert_eq!(count(tenant).await, 2);
}

#[tokio::test]
async fn test_link_failures_before_the_database() {
    let tenant = tenant().await;
    // 本文が読めない → 400。LINE WORKS には問い合わせない
    let service = MockService::members(&members(&[]));
    let app = app_with(&service).await;
    for bad in [&b"{"[..], &br#"{"dry_run": "no"}"#[..]] {
        let res = call_raw(
            &app,
            "POST",
            LINK,
            Some(tenant),
            &[
                ("X-User-ID", "00000000-0000-0000-0000-000000000001"),
                ("X-User-Email", "admin@example.test"),
                ("X-User-Role", "admin"),
                ("content-type", "application/json"),
            ],
            bad.to_vec(),
        )
        .await;
        assert_eq!(res.status, 400);
        assert_eq!(res.json(), json!({"error": "invalid_body"}));
    }
    assert!(service.gets().is_empty());

    // メンバー一覧が取れない → 502 / 503
    let down = MockService::with(200, true);
    let app = app_with(&down).await;
    let res = send(&app, "POST", LINK, tenant, None).await;
    assert_eq!(res.status, 502);
    assert_eq!(res.json()["error"], "lineworks_unavailable");
    let app = build_lineworks(rt_client().await, None);
    let res = send(&app, "POST", LINK, tenant, None).await;
    assert_eq!(res.status, 503);
}

#[tokio::test]
async fn test_link_db_failure_is_a_500_with_a_fixed_log() {
    let tenant = tenant().await;
    let service = MockService::members(&members(&[]));
    let shared: measurements::service::SharedService = service.clone();
    let app = build_lineworks(broken_client().await, Some(shared));

    let res = send(&app, "POST", LINK, tenant, None).await;

    assert_eq!(res.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: bp settings link failed: db ("),
        "{logs:?}"
    );
}
