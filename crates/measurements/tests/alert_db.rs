//! アルコール検知メール (Refs ippoan/alc-app#436) を口から実 DB まで通す。送り手は `MockMailer` (送った 1 通ずつを記録する)。
//!
//! 条件は DB の列 `alcohol_level` (API の欄 `alcohol_value`) が **旧が NULL か 0 以下 → 新が 0 より上** になった保存だけ
//! (機器の判定 `result_type` は見ない)。宛先はテナントの notify のグループ「アルコール検知」のメンバーで、宛先ごとに 1 通。
//! ログは固定の語だけ (メールアドレス・tenant_id・氏名を出さない)。

mod support;

use std::sync::Arc;

use measurements::alert::{PendingAlert, ALERT_GROUP, PHOTO_NOTE};
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const AT: &str = "2026-09-12T15:30:00Z";
const NO_GROUP: &str = "measurements: alert skipped: no group";
const NO_RECIPIENTS: &str = "measurements: alert skipped: no recipients";
const NO_MAILER: &str = "measurements: alert skipped: no mailer";
const SEND_FAILED: &str = "measurements: alert send failed: send";

/// notify のグループを 1 つ作る (superuser で)
async fn group(tenant: Uuid, name: &str) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.notify_groups (tenant_id, name) VALUES ($1, $2) RETURNING id",
            &[&tenant, &name],
        )
        .await
        .unwrap()
        .get(0)
}

/// 宛先を 1 人作ってグループに入れる (superuser で)
async fn member(tenant: Uuid, group: Uuid, email: Option<&str>, enabled: bool) {
    let admin = admin().await;
    let id: Uuid = admin
        .query_one(
            "INSERT INTO alc_api.notify_recipients \
                 (tenant_id, name, provider, lineworks_user_id, email, enabled) \
             VALUES ($1, '受信者', 'lineworks', $2, $3, $4) RETURNING id",
            &[&tenant, &Uuid::new_v4().to_string(), &email, &enabled],
        )
        .await
        .unwrap()
        .get(0);
    admin
        .execute(
            "INSERT INTO alc_api.notify_recipient_groups (group_id, recipient_id) VALUES ($1, $2)",
            &[&group, &id],
        )
        .await
        .unwrap();
}

/// テナントと乗務員と、「アルコール検知」のグループ (メンバーは `emails` の使える宛先)
async fn setup(emails: &[&str]) -> (Uuid, Uuid) {
    let tenant = tenant().await;
    let emp = employee(
        tenant,
        "山田太郎",
        &Uuid::new_v4().simple().to_string()[..8],
    )
    .await;
    let g = group(tenant, ALERT_GROUP).await;
    for email in emails {
        member(tenant, g, Some(email), true).await;
    }
    (tenant, emp)
}

fn app_with(pg: alc_worker_db::PgClient, mailer: &Arc<MockMailer>) -> App {
    build_mailer(pg, Some(mailer.clone()))
}

/// POST (201) して応答の JSON を返す
async fn post(app: &App, tenant: Uuid, emp: Uuid, level: f64) -> Value {
    let mut body = measurement_body(emp, "pass", AT, false, None);
    body["alcohol_value"] = json!(level);
    let res = call(app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 201);
    res.json()
}

/// PUT (200)。`level` が無ければ `alcohol_value` を送らない
async fn put(app: &App, tenant: Uuid, id: &str, level: Option<f64>) -> Value {
    let mut body = completed_body("pass", AT, false, None, None);
    match level {
        Some(v) => body["alcohol_value"] = json!(v),
        None => {
            body.as_object_mut().unwrap().remove("alcohol_value");
        }
    }
    put_measurement_ok(app, tenant, id, &body).await
}

fn id_of(measurement: &Value) -> String {
    measurement["id"].as_str().unwrap().to_owned()
}

const PHOTO: &[u8] = b"\xff\xd8\xffphoto-bytes";

/// 保存時と同じ形のキー (`{tenant}/{uuid}.jpg`) と、その公開 URL (接頭辞はこの Worker が見ない)
fn photo_key(tenant: Uuid) -> String {
    format!("{tenant}/{}.jpg", Uuid::new_v4())
}

fn photo_url(key: &str) -> String {
    format!("https://files.example.test/{key}")
}

fn app_with_files(
    store: &Arc<MockStore>,
    mailer: &Arc<MockMailer>,
    pg: alc_worker_db::PgClient,
) -> App {
    let files: measurements::files::SharedStore = store.clone();
    build_full(pg, Some(files), Some(mailer.clone()))
}

/// `face_photo_url` 付きで POST (201) して応答の JSON を返す
async fn post_with_photo(app: &App, tenant: Uuid, emp: Uuid, level: f64, url: &str) -> Value {
    let mut body = measurement_body(emp, "pass", AT, false, None);
    body["alcohol_value"] = json!(level);
    body["face_photo_url"] = json!(url);
    let res = call(app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 201);
    res.json()
}

#[tokio::test]
async fn test_post_attaches_the_photo_shared_between_recipients() {
    let (tenant, emp) = setup(&["b@example.test", "a@example.test"]).await;
    let key = photo_key(tenant);
    let store = MockStore::with(&key, PHOTO);
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);

    post_with_photo(&app, tenant, emp, 0.25, &photo_url(&key)).await;

    let sent = mailer.sent();
    assert_eq!(sent.len(), 2);
    for mail in &sent {
        let a = mail.attachment.as_ref().expect("写真が付く");
        assert_eq!((a.filename, a.content_type), ("photo.jpg", "image/jpeg"));
        assert_eq!(&a.bytes[..], PHOTO);
        assert!(mail.body.ends_with(PHOTO_NOTE), "{}", mail.body);
    }
    // 宛先ごとに複製せず共有する。R2 の読みは 1 回
    let (x, y) = (sent[0].attachment.as_ref(), sent[1].attachment.as_ref());
    assert!(Arc::ptr_eq(&x.unwrap().bytes, &y.unwrap().bytes));
    assert_eq!(store.gets(), [key]);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_response_returns_before_the_photo_is_read_and_the_mail_is_sent() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let key = photo_key(tenant);
    let store = MockStore::with(&key, PHOTO);
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);
    let mut body = measurement_body(emp, "pass", AT, false, None);
    body["alcohol_value"] = json!(0.25);
    body["face_photo_url"] = json!(photo_url(&key));
    let headers = [("content-type", "application/json")];
    let bytes = serde_json::to_vec(&body).unwrap();

    let mut resp = respond_raw(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        &headers,
        bytes,
    )
    .await;

    // 応答の時点では写真を読まず送っていない。後で送るものは応答の extension に在る
    assert_eq!(resp.status(), 201);
    assert!(mailer.sent().is_empty());
    assert!(store.gets().is_empty());
    let pending = resp.extensions_mut().remove::<PendingAlert>();
    let pending = pending.expect("検知の保存は後で送るものを応答に載せる");
    assert_eq!(pending.send().await, (1, 0));
    let sent = mailer.sent();
    assert_eq!(sent.len(), 1);
    assert!(sent[0].attachment.is_some());
    assert_eq!(store.gets(), [key]);

    // 送らない保存 (値 0) は何も載せない
    let mut zero = measurement_body(emp, "pass", AT, false, None);
    zero["alcohol_value"] = json!(0.0);
    let bytes = serde_json::to_vec(&zero).unwrap();
    let resp = respond_raw(
        &app,
        "POST",
        "/api/measurements",
        Some(tenant),
        &headers,
        bytes,
    )
    .await;
    assert_eq!(resp.status(), 201);
    assert!(resp.extensions().get::<PendingAlert>().is_none());
}

#[tokio::test]
async fn test_put_attaches_the_photo_saved_by_the_same_put() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let key = photo_key(tenant);
    let store = MockStore::with(&key, PHOTO);
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);
    let id = start_measurement(&app, tenant, emp).await;

    let mut body = completed_body("pass", AT, false, None, None);
    body["alcohol_value"] = json!(0.1);
    body["face_photo_url"] = json!(photo_url(&key));
    put_measurement_ok(&app, tenant, &id, &body).await;

    let sent = mailer.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(&sent[0].attachment.as_ref().unwrap().bytes[..], PHOTO);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_without_a_photo_url_the_mail_has_no_attachment() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let store = Arc::new(MockStore::default());
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);

    post(&app, tenant, emp, 0.25).await;

    let mail = &mailer.sent()[0];
    assert_eq!(mail.attachment, None);
    assert!(!mail.body.contains(PHOTO_NOTE));
    assert!(store.gets().is_empty());
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_unreadable_photo_sends_the_body_only_and_logs_a_fixed_word() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    // 形は正しいが R2 に無いキー
    let key = photo_key(tenant);
    let store = Arc::new(MockStore::default());
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);

    post_with_photo(&app, tenant, emp, 0.25, &photo_url(&key)).await;

    assert_eq!(mailer.sent_to(), ["a@example.test"]);
    assert_eq!(mailer.sent()[0].attachment, None);
    assert!(!mailer.sent()[0].body.contains(PHOTO_NOTE));
    assert_eq!(
        app.logs(),
        ["measurements: alert photo failed: storage (missing)"]
    );
}

#[tokio::test]
async fn test_bad_or_other_tenant_photo_url_is_not_read() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let other = Uuid::new_v4();
    // 別 tenant の写真は R2 に在っても読まない
    let other_key = photo_key(other);
    let store = MockStore::with(&other_key, PHOTO);
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);

    post_with_photo(&app, tenant, emp, 0.25, &photo_url(&other_key)).await;
    post_with_photo(
        &app,
        tenant,
        emp,
        0.25,
        "https://files.example.test/not-a-key",
    )
    .await;

    assert_eq!(mailer.sent().len(), 2);
    assert!(mailer.sent().iter().all(|m| m.attachment.is_none()));
    assert!(store.gets().is_empty(), "R2 は読まない");
    assert_eq!(app.logs(), ["measurements: alert photo failed: key"; 2]);
}

#[tokio::test]
async fn test_no_file_store_sends_the_body_only_without_a_log() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let url = photo_url(&photo_key(tenant));

    post_with_photo(&app, tenant, emp, 0.25, &url).await;

    assert_eq!(mailer.sent()[0].attachment, None);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_photo_is_not_read_when_no_mail_is_sent() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let key = photo_key(tenant);
    let store = MockStore::with(&key, PHOTO);
    let mailer = Arc::new(MockMailer::default());
    let app = app_with_files(&store, &mailer, rt_client().await);

    // 値 0 は検知ではない。送らないので写真も読まない
    post_with_photo(&app, tenant, emp, 0.0, &photo_url(&key)).await;

    assert!(mailer.sent().is_empty());
    assert!(store.gets().is_empty());
}

#[tokio::test]
async fn test_post_above_zero_sends_one_mail_per_recipient() {
    let (tenant, emp) = setup(&["b@example.test", "a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    // 機器の判定が pass でも、値が 0 より上なら送る (見るのは値)
    let m = post(&app, tenant, emp, 0.25).await;

    assert_eq!(mailer.sent_to(), ["a@example.test", "b@example.test"]);
    let mail = &mailer.sent()[0];
    assert_eq!(mail.subject, "アルコール検知: 山田太郎");
    assert!(mail.body.contains("乗務員: 山田太郎\n"), "{}", mail.body);
    assert!(mail.body.contains("アルコール値: 0.25\n"));
    assert!(mail
        .body
        .contains("測定日時: 2026-09-13 00:30:00 (日本時間)\n"));
    assert!(mail.body.contains(&format!("測定 ID: {}\n", id_of(&m))));
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_post_zero_does_not_send() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    post(&app, tenant, emp, 0.0).await;

    assert!(mailer.attempted().is_empty());
    assert!(app.logs().is_empty(), "条件を満たさなければ宛先も引かない");
}

#[tokio::test]
async fn test_start_without_a_value_does_not_send() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    start_measurement(&app, tenant, emp).await;

    assert!(mailer.attempted().is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_put_null_to_above_zero_sends() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let id = start_measurement(&app, tenant, emp).await;

    let res = put(&app, tenant, &id, Some(0.1)).await;

    assert_eq!(res["alcohol_value"], json!(0.1));
    assert_eq!(mailer.sent_to(), ["a@example.test"]);
    assert!(mailer.sent()[0].body.contains(&format!("測定 ID: {id}\n")));
}

#[tokio::test]
async fn test_put_zero_to_above_zero_sends() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let id = id_of(&post(&app, tenant, emp, 0.0).await);
    assert!(mailer.attempted().is_empty());

    put(&app, tenant, &id, Some(0.1)).await;

    assert_eq!(mailer.sent_to(), ["a@example.test"]);
}

#[tokio::test]
async fn test_put_above_zero_to_above_zero_does_not_send_again() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let id = id_of(&post(&app, tenant, emp, 0.2).await);
    assert_eq!(mailer.sent().len(), 1);

    // 同じ測定を >0 のまま保存し直しても、値が変わっても 1 通のまま
    put(&app, tenant, &id, Some(0.2)).await;
    put(&app, tenant, &id, Some(0.3)).await;

    assert_eq!(mailer.sent().len(), 1);
}

#[tokio::test]
async fn test_put_above_zero_to_zero_does_not_send_and_a_new_rise_does() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let id = id_of(&post(&app, tenant, emp, 0.2).await);
    assert_eq!(mailer.sent().len(), 1);

    put(&app, tenant, &id, Some(0.0)).await;
    assert_eq!(mailer.sent().len(), 1, "0 に戻す保存では送らない");

    // 戻したあとにもう一度 0 より上になった回は、新しい通知
    put(&app, tenant, &id, Some(0.4)).await;
    assert_eq!(mailer.sent().len(), 2);
}

#[tokio::test]
async fn test_put_without_a_value_keeps_the_old_one_and_does_not_send() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    // 旧値が > 0: 値を送らない PUT は COALESCE で旧値が残る (入力に値が無いので「上がった」ではない)
    let id = id_of(&post(&app, tenant, emp, 0.2).await);
    let res = put(&app, tenant, &id, None).await;
    assert_eq!(res["alcohol_value"], json!(0.2));
    assert_eq!(mailer.sent().len(), 1);

    // 旧値が NULL: 値を送らない PUT は NULL のまま
    let started = start_measurement(&app, tenant, emp).await;
    let res = put(&app, tenant, &started, None).await;
    assert_eq!(res["alcohol_value"], Value::Null);
    assert_eq!(mailer.sent().len(), 1);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_missing_group_logs_a_distinct_word() {
    // グループ「アルコール検知」が無い = 設定漏れ。メンバー 0 とは別の語で見分ける
    let tenant = tenant().await;
    let emp = employee(
        tenant,
        "山田太郎",
        &Uuid::new_v4().simple().to_string()[..8],
    )
    .await;
    group(tenant, "別のグループ").await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    post(&app, tenant, emp, 0.2).await;

    assert!(mailer.attempted().is_empty());
    assert_eq!(app.logs(), [NO_GROUP]);
}

#[tokio::test]
async fn test_group_without_members_logs_no_recipients() {
    let (tenant, emp) = setup(&[]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    post(&app, tenant, emp, 0.2).await;

    assert!(mailer.attempted().is_empty());
    assert_eq!(app.logs(), [NO_RECIPIENTS]);
}

#[tokio::test]
async fn test_unusable_recipients_are_skipped() {
    let tenant = tenant().await;
    let emp = employee(
        tenant,
        "山田太郎",
        &Uuid::new_v4().simple().to_string()[..8],
    )
    .await;
    let g = group(tenant, ALERT_GROUP).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    // email が NULL・空・無効 (enabled = false) の宛先だけなら送らない
    member(tenant, g, None, true).await;
    member(tenant, g, Some(""), true).await;
    member(tenant, g, Some("off@example.test"), false).await;
    post(&app, tenant, emp, 0.2).await;
    assert!(mailer.attempted().is_empty());
    assert_eq!(app.logs(), [NO_RECIPIENTS]);

    // 使える宛先が加われば、その人だけに 1 通 (同じ email の宛先が 2 つ在っても 1 通)
    member(tenant, g, Some("ok@example.test"), true).await;
    member(tenant, g, Some("ok@example.test"), true).await;
    post(&app, tenant, emp, 0.2).await;
    assert_eq!(mailer.sent_to(), ["ok@example.test"]);
}

#[tokio::test]
async fn test_send_failure_keeps_the_save_and_reaches_the_others() {
    let (tenant, emp) = setup(&["bad@example.test", "good@example.test"]).await;
    let mailer = MockMailer::failing(&["bad@example.test"]);
    let app = app_with(rt_client().await, &mailer);

    // POST は 201 のまま。失敗した 1 人のあとも、ほかの宛先へは送る
    let m = post(&app, tenant, emp, 0.2).await;
    assert_eq!(
        mailer.attempted(),
        ["bad@example.test", "good@example.test"]
    );
    assert_eq!(mailer.sent_to(), ["good@example.test"]);
    assert_eq!(app.logs(), [SEND_FAILED]);
    assert_eq!(measurement_count(tenant).await, 1);
    assert_eq!(measurement_status(&id_of(&m)).await, "completed");

    // PUT も 200 のまま
    let id = start_measurement(&app, tenant, emp).await;
    put(&app, tenant, &id, Some(0.3)).await;
    assert_eq!(app.logs(), [SEND_FAILED, SEND_FAILED]);
    let logs = app.logs().join("\n");
    assert!(!logs.contains("example.test") && !logs.contains(&tenant.to_string()));
}

#[tokio::test]
async fn test_no_mailer_does_not_send_and_logs_once() {
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let app = build_mailer(rt_client().await, None);

    // 保存はそのまま。条件を満たす保存だけが 1 行ログを残す
    post(&app, tenant, emp, 0.0).await;
    assert!(app.logs().is_empty());
    post(&app, tenant, emp, 0.2).await;
    assert_eq!(app.logs(), [NO_MAILER]);
    assert_eq!(measurement_count(tenant).await, 2);
}

#[tokio::test]
async fn test_another_tenants_group_is_not_used() {
    let (_tenant_a, _) = setup(&["a@example.test"]).await;
    // B にはグループが無い。A のグループ・宛先は引かない (tenant_id を条件にし、RLS も効く)
    let tenant_b = tenant().await;
    let emp_b = employee(tenant_b, "鈴木", &Uuid::new_v4().simple().to_string()[..8]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    post(&app, tenant_b, emp_b, 0.2).await;

    assert!(mailer.attempted().is_empty());
    assert_eq!(app.logs(), [NO_GROUP]);
}

#[tokio::test]
async fn test_lookup_failure_keeps_the_save_and_logs_a_fixed_word() {
    // 宛先の引きが失敗しても (この tenant のグループの行を読むと必ず失敗する制限的な policy を張る)、保存は残り、
    // ログは固定の語 + SQLSTATE だけで、メールは送らない。保存の transaction とは別なので巻き戻らない
    let (tenant, emp) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);
    let su = admin().await;
    let policy = format!("alert_fail_{}", tenant.simple());
    su.batch_execute(&format!(
        "CREATE POLICY {policy} ON alc_api.notify_groups AS RESTRICTIVE FOR SELECT \
         USING (CASE WHEN tenant_id = '{tenant}'::uuid THEN 1 / (length(name) - length(name)) = 1 ELSE true END)"
    ))
    .await
    .unwrap();

    let m = post(&app, tenant, emp, 0.2).await;

    su.batch_execute(&format!("DROP POLICY {policy} ON alc_api.notify_groups"))
        .await
        .unwrap();
    assert!(mailer.attempted().is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: alert lookup failed: db ("),
        "{logs:?}"
    );
    assert!(!logs[0].contains(&tenant.to_string()));
    assert_eq!(measurement_status(&id_of(&m)).await, "completed");
}

// ---- テスト送信 (`POST /api/measurements/alert-test`。admin だけ。測定は起こさない) ----

const ALERT_TEST: &str = "/api/measurements/alert-test";

/// テスト送信を叩く。`role` があれば X-User-ID・X-User-Email・X-User-Role を揃えて付ける (揃わないと AuthUser が無い)
async fn alert_test(app: &App, tenant: Uuid, role: Option<&str>) -> Got {
    let user = Uuid::new_v4().to_string();
    let headers: Vec<(&str, &str)> = match role {
        Some(r) => vec![
            ("X-User-ID", user.as_str()),
            ("X-User-Email", "admin@example.test"),
            ("X-User-Role", r),
        ],
        None => vec![],
    };
    call_raw(app, "POST", ALERT_TEST, Some(tenant), &headers, Vec::new()).await
}

#[tokio::test]
async fn test_alert_test_is_admin_only() {
    let (tenant, _) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    // admin でない role・ヘッダ無し・role だけ (X-User-ID が無く AuthUser が復元されない) は 403
    for res in [
        alert_test(&app, tenant, Some("viewer")).await,
        alert_test(&app, tenant, None).await,
        call_raw(
            &app,
            "POST",
            ALERT_TEST,
            Some(tenant),
            &[("X-User-Role", "admin")],
            Vec::new(),
        )
        .await,
    ] {
        assert_eq!(res.status, 403);
        assert_eq!(res.json(), json!({ "error": "forbidden" }));
    }
    assert!(mailer.attempted().is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn test_alert_test_without_mailer_is_503() {
    let (tenant, _) = setup(&["a@example.test"]).await;
    let app = build_mailer(rt_client().await, None);

    let res = alert_test(&app, tenant, Some("admin")).await;

    assert_eq!(res.status, 503);
    assert_eq!(res.json(), json!({ "error": "mail_unavailable" }));
}

#[tokio::test]
async fn test_alert_test_without_recipients_is_409() {
    let tenant = tenant().await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    // グループが無い
    let res = alert_test(&app, tenant, Some("admin")).await;
    assert_eq!(res.status, 409);
    assert_eq!(res.json(), json!({ "error": "no_recipients" }));

    // グループは在るが、使える宛先 (enabled・email あり) が居ない
    let g = group(tenant, ALERT_GROUP).await;
    member(tenant, g, None, true).await;
    member(tenant, g, Some("off@example.test"), false).await;
    let res = alert_test(&app, tenant, Some("admin")).await;
    assert_eq!(res.status, 409);
    assert_eq!(res.json(), json!({ "error": "no_recipients" }));
    assert!(mailer.attempted().is_empty());
}

#[tokio::test]
async fn test_alert_test_sends_one_test_mail_per_usable_recipient() {
    let (tenant, _) = setup(&["b@example.test", "a@example.test"]).await;
    let g: Uuid = admin()
        .await
        .query_one(
            "SELECT id FROM alc_api.notify_groups WHERE tenant_id = $1 AND name = $2",
            &[&tenant, &ALERT_GROUP],
        )
        .await
        .unwrap()
        .get(0);
    member(tenant, g, Some("off@example.test"), false).await;
    member(tenant, g, None, true).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(rt_client().await, &mailer);

    let res = alert_test(&app, tenant, Some("admin")).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.json(), json!({ "sent": 2, "failed": 0 }));
    assert_eq!(mailer.attempted(), ["a@example.test", "b@example.test"]);
    for mail in mailer.sent() {
        assert!(mail.subject.starts_with("【テスト】"), "{}", mail.subject);
        assert_eq!(mail.subject, "【テスト】アルコール検知: テスト送信");
        assert!(mail.body.contains("テスト送信"), "{}", mail.body);
        assert!(!mail.body.contains("乗務員") && !mail.body.contains("アルコール値"));
        // テスト送信に写真は付けない
        assert_eq!(mail.attachment, None);
        assert!(!mail.body.contains(PHOTO_NOTE));
    }
    // 測定は起こさない
    assert_eq!(measurement_count(tenant).await, 0);
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn test_alert_test_counts_a_failed_send() {
    let (tenant, _) = setup(&["bad@example.test", "good@example.test"]).await;
    let mailer = MockMailer::failing(&["bad@example.test"]);
    let app = app_with(rt_client().await, &mailer);

    let res = alert_test(&app, tenant, Some("admin")).await;

    // 宛先ごとの成否・アドレスは返さない
    assert_eq!(res.status, 200);
    assert_eq!(res.json(), json!({ "sent": 1, "failed": 1 }));
    assert_eq!(mailer.sent_to(), ["good@example.test"]);
    assert_eq!(app.logs(), [SEND_FAILED]);
}

#[tokio::test]
async fn test_alert_test_lookup_failure_is_500_with_a_fixed_word() {
    let (tenant, _) = setup(&["a@example.test"]).await;
    let mailer = Arc::new(MockMailer::default());
    let app = app_with(broken_client().await, &mailer);

    let res = alert_test(&app, tenant, Some("admin")).await;

    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    assert!(mailer.attempted().is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with("measurements: alert test lookup failed: db ("),
        "{logs:?}"
    );
    assert!(!logs[0].contains(&tenant.to_string()));
}
