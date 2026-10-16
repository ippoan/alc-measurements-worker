//! 実 DB のテストの準備・接続 (ippoan/alc-trouble-worker の tests/support の型)。
//!
//! - **`MEASUREMENTS_TEST_ADMIN_DATABASE_URL` が未設定なら失敗する** (skip して緑にしない)。値は準備用の superuser の
//!   接続文字列で、**使い捨ての DB (database 名 `postgres`) に向けること** (ロール・schema・全 migration を作る)。表示はしない
//! - 準備の SQL は ippoan/alc-migrations から版を固定して取る (`scripts/ALC_MIGRATIONS_REV`、
//!   `bash scripts/fetch-migrations.sh` が repo 直下の `.alc-migrations/` に取り出す)。流す順は init_local_db.sql →
//!   空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) → local_app_grants.sql
//! - 本体のテストは、本番の worker と同じ実行用ロール `alc_api_rt` (migration 158。表の所有者ではない・`NOSUPERUSER`・
//!   `NOBYPASSRLS`) で繋ぐ ([`client_as`] が毎回確かめる)。パスワードは使い捨ての DB 専用の固定の文字列で、秘密ではない

#![allow(dead_code)]

use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::Router;
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use measurements::files::{FileStore, SharedStore, StoreError};
use measurements::{router, AppState};
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio_postgres::{Client, Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

pub const ADMIN_URL_ENV: &str = "MEASUREMENTS_TEST_ADMIN_DATABASE_URL";
pub const RT_ROLE: &str = "alc_api_rt";
/// 準備が済んだ印のロール (ログインはしない。別の binary が流した準備を見分けるだけ)
pub const PREPARED_ROLE: &str = "measurements_test_prepared";
/// 使い捨ての DB 専用 (秘密ではない)
const PASSWORD: &str = "measurements-test-throwaway";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";
/// 準備を直列にする advisory lock の鍵 (並列に走るテストの binary どうし)
const PREPARE_LOCK: i64 = 1_012_436;

/// 口が触る表 (実行用ロールがどれの所有者でもないことを確かめる)
pub const TABLES: &[&str] = &[
    "measurements",
    "tenko_sessions",
    "tenko_records",
    "employees",
    "car_inspection",
];

fn migrations_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        ".alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

pub fn admin_config() -> Config {
    let url = std::env::var(ADMIN_URL_ENV).unwrap_or_else(|_| {
        panic!("{ADMIN_URL_ENV} が未設定。実 DB のテストは接続先が無ければ失敗させる (skip しない)")
    });
    url.parse()
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} が接続文字列として読めない"))
}

async fn raw_connect(config: &Config) -> Client {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} の DB に繋げない"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// superuser の接続 (準備と、テストの行を作る・見るのだけに使う。RLS を通らない)
pub async fn admin() -> Client {
    let mut config = admin_config();
    config.options(SEARCH_PATH);
    raw_connect(&config).await
}

/// 準備をプロセスの中で 1 回だけ流す。既に印のロールが在れば (別の binary が流した) 何もしない
pub async fn prepare() {
    static PREPARED: OnceCell<()> = OnceCell::const_new();
    PREPARED
        .get_or_init(|| async {
            let su = admin().await;
            su.batch_execute(&format!("SELECT pg_advisory_lock({PREPARE_LOCK})"))
                .await
                .unwrap();
            let done = su
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[&PREPARED_ROLE],
                )
                .await
                .unwrap()
                .get::<_, bool>(0);
            if !done {
                migrate(&su).await;
            }
            su.batch_execute(&format!("SELECT pg_advisory_unlock({PREPARE_LOCK})"))
                .await
                .unwrap();
            eprintln!(
                "[measurements-db] 実 DB で準備済み (migrations は {})",
                rev()
            );
        })
        .await;
}

fn rev() -> String {
    std::fs::read_to_string(migrations_dir().join(".rev"))
        .unwrap()
        .trim()
        .to_owned()
}

async fn migrate(su: &Client) {
    let dir = migrations_dir();
    let read = |p: std::path::PathBuf| std::fs::read_to_string(p).unwrap();
    su.batch_execute(&read(dir.join("scripts/init_local_db.sql")))
        .await
        .unwrap();
    su.batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("migrations"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    for needed in ["_runtime_role.sql", "_tenko_sessions_identity_method.sql"] {
        assert!(
            files
                .iter()
                .any(|f| f.file_name().unwrap().to_string_lossy().ends_with(needed)),
            "取り出した migrations に *{needed} が無い (scripts/ALC_MIGRATIONS_REV が古い)"
        );
    }
    for file in files {
        let name = file.file_name().unwrap().to_owned();
        if let Err(e) = su
            .batch_execute(&format!("BEGIN;\n{}\n;COMMIT;", read(file)))
            .await
        {
            panic!("migration {name:?}: {:?}", e.code());
        }
    }
    su.batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
        .await
        .unwrap();
    // 実行用ロール (158 が NOLOGIN で作る) にログインを付け、準備の印を立てる
    su.batch_execute(&format!(
        "ALTER ROLE {RT_ROLE} LOGIN PASSWORD '{PASSWORD}';
         CREATE ROLE {PREPARED_ROLE} NOLOGIN;"
    ))
    .await
    .unwrap();
}

/// `role` の接続 (worker と同じく `PgClient` に包む)。**superuser / BYPASSRLS / 触る表の所有者なら panic**
pub async fn client_as(role: &str) -> PgClient {
    prepare().await;
    let mut config = admin_config();
    config.user(role).password(PASSWORD);
    let client = raw_connect(&config).await;
    let row = client
        .query_one(
            "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
             (SELECT coalesce(bool_or(tableowner = current_user), false) FROM pg_tables \
              WHERE schemaname = 'alc_api' AND tablename = ANY($1))",
            &[&TABLES],
        )
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0), "RLS を素通りするロールで繋いでいる");
    assert!(!row.get::<_, bool>(1), "表の所有者で繋いでいる");
    PgClient::new(client)
}

pub async fn rt_client() -> PgClient {
    client_as(RT_ROLE).await
}

/// 接続が切れた `alc_api_rt` の接続 (backend を落とした接続)
pub async fn broken_client() -> PgClient {
    let mut pg = rt_client().await;
    let pid: i32 = pg
        .tenant_tx(Uuid::nil(), |tx| {
            Box::pin(async move {
                let row = tx.query_typed_one("SELECT pg_backend_pid()", &[]).await?;
                Ok(row.get::<_, i32>(0))
            })
        })
        .await
        .unwrap();
    let terminated: bool = admin()
        .await
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
    pg
}

/// tenant を 1 つ作る (superuser で。名前は乱数)
pub async fn tenant() -> Uuid {
    prepare().await;
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenants (id, name) VALUES ($1, $2)",
            &[&id, &format!("measurements-test-{id}")],
        )
        .await
        .unwrap();
    id
}

/// 乗務員を 1 人作る (superuser で。rust のテストの `create_test_employee` の代わり)。`nfc_id` は乱数
pub async fn employee(tenant: Uuid, name: &str, code: &str) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, nfc_id, name, code) VALUES ($1, $2, $3, $4, $5)",
            &[&id, &tenant, &id.to_string(), &name, &code],
        )
        .await
        .unwrap();
    id
}

/// car_inspection に合成の行を 1 件入れる (superuser で。rust のテストの `insert_car_inspection` と同じ値。
/// 取り込みの UPSERT は rust に在るので、ここでは NOT NULL の残りの列を空文字で埋めて直接入れる)
pub async fn car_inspection(
    tenant: Uuid,
    cert_no: &str,
    car_id: &str,
    expirdate: &str,
    grantdate_d: &str,
) {
    let su = admin().await;
    let given: [(&str, String); 8] = [
        ("ElectCertMgNo", cert_no.to_owned()),
        ("CarId", car_id.to_owned()),
        ("EntryNoCarNo", format!("TEST-CAR-NO-{cert_no}")),
        ("GrantdateE", "R".to_owned()),
        ("GrantdateY", "07".to_owned()),
        ("GrantdateM", "01".to_owned()),
        ("GrantdateD", grantdate_d.to_owned()),
        (
            "TwodimensionCodeInfoValidPeriodExpirdate",
            expirdate.to_owned(),
        ),
    ];
    // 既定値の無い NOT NULL の text 列 (上で与えるもの以外) は空文字
    let rest: Vec<String> = su
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'alc_api' AND table_name = 'car_inspection' \
               AND is_nullable = 'NO' AND column_default IS NULL AND data_type = 'text'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|c| given.iter().all(|(g, _)| g != c))
        .collect();
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let mut cols = vec!["tenant_id".to_owned()];
    let mut vals = vec![quote(&tenant.to_string())];
    for (c, v) in &given {
        cols.push(format!("\"{c}\""));
        vals.push(quote(v));
    }
    for c in &rest {
        cols.push(format!("\"{c}\""));
        vals.push("''".to_owned());
    }
    su.batch_execute(&format!(
        "INSERT INTO alc_api.car_inspection ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    ))
    .await
    .expect("car_inspection の合成行を入れられない");
}

pub struct App {
    pub router: Router,
    pub logs: Arc<StdMutex<Vec<String>>>,
}

impl App {
    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }
}

/// R2 のモック。`objects` に在るキーだけ返し、無いキーは失敗 (本物の R2 の「オブジェクト無し」と同じ)。
/// 読まれたキーを `gets` に残す
#[derive(Default)]
pub struct MockStore {
    pub objects: StdMutex<std::collections::HashMap<String, Vec<u8>>>,
    pub gets: StdMutex<Vec<String>>,
}

impl MockStore {
    pub fn with(key: &str, bytes: &[u8]) -> Arc<Self> {
        let store = Self::default();
        store
            .objects
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.to_vec());
        Arc::new(store)
    }

    pub fn gets(&self) -> Vec<String> {
        self.gets.lock().unwrap().clone()
    }
}

impl FileStore for MockStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            self.gets.lock().unwrap().push(key.to_owned());
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or(StoreError("missing"))
        })
    }
}

/// worker の src/lib.rs と同じ組み立て (R2 は無し)
pub fn build(pg: PgClient) -> App {
    build_with(pg, None)
}

/// worker の src/lib.rs と同じ組み立て。`files` は R2 の置き場 (無ければ顔写真・動画の口は 503)
pub fn build_with(pg: PgClient, files: Option<SharedStore>) -> App {
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        files,
    };
    App {
        router: router().with_state(state),
        logs,
    }
}

pub async fn app() -> App {
    build(rt_client().await)
}

pub struct Got {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Got {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// 1 リクエストを打つ。`tenant` が在れば `X-Tenant-ID` を付ける。`body` が在れば JSON で送る
pub async fn call(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    body: Option<Value>,
) -> Got {
    match body {
        Some(v) => {
            let bytes = serde_json::to_vec(&v).unwrap();
            call_raw(
                app,
                method,
                uri,
                tenant,
                &[("content-type", "application/json")],
                bytes,
            )
            .await
        }
        None => call_raw(app, method, uri, tenant, &[], Vec::new()).await,
    }
}

/// 本文とヘッダーをそのまま送る
pub async fn call_raw(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    headers: &[(&str, &str)],
    body: Vec<u8>,
) -> Got {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        b = b.header("X-Tenant-ID", t.to_string());
    }
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got {
        status,
        headers,
        body,
    }
}

// --- 測定の口を打つ道具 (rust の tests/normal_tenko_record_test.rs の helper の写し) ---

/// 測定の POST の本文 (rust の `send_measurement` と同じ形。`tenko_type` を付けられる)
pub fn measurement_body(
    employee_id: Uuid,
    result_type: &str,
    measured_at: &str,
    record_as_tenko: bool,
    tenko_type: Option<&str>,
) -> Value {
    let mut body = serde_json::json!({
        "employee_id": employee_id,
        "alcohol_value": 0.0,
        "result_type": result_type,
        "measured_at": measured_at,
        "temperature": 36.5,
        "systolic": 120,
        "diastolic": 80,
        "pulse": 64,
        "record_as_tenko": record_as_tenko,
    });
    if let Some(tt) = tenko_type {
        body["tenko_type"] = Value::from(tt);
    }
    body
}

/// 測定の POST を送り、応答をそのまま返す
pub async fn send_measurement(
    app: &App,
    tenant: Uuid,
    employee_id: Uuid,
    result_type: &str,
    measured_at: &str,
    record_as_tenko: bool,
    tenko_type: Option<&str>,
) -> Got {
    let body = measurement_body(
        employee_id,
        result_type,
        measured_at,
        record_as_tenko,
        tenko_type,
    );
    call(app, "POST", "/api/measurements", Some(tenant), Some(body)).await
}

/// 測定を 1 件保存する (201 を確かめて応答の JSON を返す)
pub async fn post_measurement(
    app: &App,
    tenant: Uuid,
    employee_id: Uuid,
    result_type: &str,
    measured_at: &str,
    record_as_tenko: bool,
) -> Value {
    let res = send_measurement(
        app,
        tenant,
        employee_id,
        result_type,
        measured_at,
        record_as_tenko,
        None,
    )
    .await;
    assert_eq!(res.status, 201, "measurement POST failed");
    res.json()
}

/// 測定を開始して id を返す
pub async fn start_measurement(app: &App, tenant: Uuid, employee_id: Uuid) -> String {
    let res = call(
        app,
        "POST",
        "/api/measurements/start",
        Some(tenant),
        Some(serde_json::json!({ "employee_id": employee_id })),
    )
    .await;
    assert_eq!(res.status, 201, "measurement start failed");
    res.json()["id"].as_str().unwrap().to_string()
}

/// 完了 PUT の body (`tenko_type` / `tenko_method` は付けたときだけ載る)
pub fn completed_body(
    result_type: &str,
    measured_at: &str,
    record_as_tenko: bool,
    tenko_type: Option<&str>,
    tenko_method: Option<&str>,
) -> Value {
    let mut body = serde_json::json!({
        "status": "completed",
        "alcohol_value": 0.0,
        "result_type": result_type,
        "measured_at": measured_at,
        "temperature": 36.5,
        "record_as_tenko": record_as_tenko,
    });
    if let Some(tt) = tenko_type {
        body["tenko_type"] = Value::from(tt);
    }
    if let Some(method) = tenko_method {
        body["tenko_method"] = Value::from(method);
    }
    body
}

/// PUT を送り、応答をそのまま返す
pub async fn put_measurement(app: &App, tenant: Uuid, measurement_id: &str, body: &Value) -> Got {
    call(
        app,
        "PUT",
        &format!("/api/measurements/{measurement_id}"),
        Some(tenant),
        Some(body.clone()),
    )
    .await
}

/// PUT を送り、200 の応答 JSON を返す
pub async fn put_measurement_ok(
    app: &App,
    tenant: Uuid,
    measurement_id: &str,
    body: &Value,
) -> Value {
    let res = put_measurement(app, tenant, measurement_id, body).await;
    assert_eq!(res.status, 200, "measurement PUT failed");
    res.json()
}

fn uuid_of(s: &str) -> Uuid {
    Uuid::parse_str(s).unwrap()
}

/// 測定に紐づく session / record の (tenko_type, tenko_method)
pub async fn type_and_method(
    tenant: Uuid,
    measurement_id: &str,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let su = admin().await;
    let mid = uuid_of(measurement_id);
    let sessions = su
        .query(
            "SELECT tenko_type, tenko_method FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let records = su
        .query(
            "SELECT r.tenko_type, r.tenko_method FROM alc_api.tenko_records r
             JOIN alc_api.tenko_sessions s ON s.id = r.session_id
             WHERE r.tenant_id = $1 AND s.measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    (sessions, records)
}

/// テナント内の測定の件数
pub async fn measurement_count(tenant: Uuid) -> i64 {
    admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.measurements WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0)
}

/// 測定の status (superuser で直接)
pub async fn measurement_status(measurement_id: &str) -> String {
    admin()
        .await
        .query_one(
            "SELECT status FROM alc_api.measurements WHERE id = $1",
            &[&uuid_of(measurement_id)],
        )
        .await
        .unwrap()
        .get(0)
}

/// 通常点呼のセッションを測定 id で引く (tenko_type, status, cancel_reason)
pub async fn sessions_for_measurement(
    tenant: Uuid,
    measurement_id: &str,
) -> Vec<(String, String, Option<String>)> {
    admin()
        .await
        .query(
            "SELECT tenko_type, status, cancel_reason FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND measurement_id = $2",
            &[&tenant, &uuid_of(measurement_id)],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// テナント内の '通常点呼' の点呼セッション / 点呼記録の件数
pub async fn counts(tenant: Uuid) -> (i64, i64) {
    let su = admin().await;
    let sessions: i64 = su
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND tenko_method = '通常点呼'",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    let records: i64 = su
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_records WHERE tenant_id = $1 AND tenko_method = '通常点呼'",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    (sessions, records)
}

/// テナント内の点呼セッション / 点呼記録の件数 (点呼方法を問わない)
pub async fn counts_any_method(tenant: Uuid) -> (i64, i64) {
    let su = admin().await;
    let sessions: i64 = su
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    let records: i64 = su
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_records WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0);
    (sessions, records)
}

/// 測定に紐づく session の (id, tenko_type, tenko_method, status)
pub async fn session_rows(
    tenant: Uuid,
    measurement_id: &str,
) -> Vec<(Uuid, String, String, String)> {
    admin()
        .await
        .query(
            "SELECT id, tenko_type, tenko_method, status FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND measurement_id = $2",
            &[&tenant, &uuid_of(measurement_id)],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// 測定に紐づく session の identity_method と、record の record_data の同じ値。record 側は (key が在るか, 値)
pub async fn identity_methods(
    tenant: Uuid,
    measurement_id: &str,
) -> (Vec<Option<String>>, Vec<(bool, Option<String>)>) {
    let su = admin().await;
    let mid = uuid_of(measurement_id);
    let sessions = su
        .query(
            "SELECT identity_method FROM alc_api.tenko_sessions
             WHERE tenant_id = $1 AND measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    let records = su
        .query(
            "SELECT r.record_data ? 'identity_method', r.record_data->>'identity_method'
             FROM alc_api.tenko_records r
             JOIN alc_api.tenko_sessions s ON s.id = r.session_id
             WHERE r.tenant_id = $1 AND s.measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    (sessions, records)
}

pub type CarinsCols = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// 測定に紐づく session の 4 列と、record の record_data の同じ 4 値
pub async fn carins_of(tenant: Uuid, measurement: &Value) -> (CarinsCols, CarinsCols) {
    let su = admin().await;
    let mid = uuid_of(measurement["id"].as_str().unwrap());
    let s = su
        .query_one(
            "SELECT carins_cert_no, carins_vehicle_id, carins_expires_on::text, carins_matched_by
             FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap();
    let r = su
        .query_one(
            "SELECT r.record_data->>'carins_cert_no', r.record_data->>'carins_vehicle_id',
                    r.record_data->>'carins_expires_on', r.record_data->>'carins_matched_by'
             FROM alc_api.tenko_records r
             JOIN alc_api.tenko_sessions s ON s.id = r.session_id
             WHERE r.tenant_id = $1 AND s.measurement_id = $2",
            &[&tenant, &mid],
        )
        .await
        .unwrap();
    (
        (s.get(0), s.get(1), s.get(2), s.get(3)),
        (r.get(0), r.get(1), r.get(2), r.get(3)),
    )
}

pub fn cols(
    cert_no: Option<&str>,
    vehicle_id: Option<&str>,
    expires_on: Option<&str>,
    matched_by: Option<&str>,
) -> CarinsCols {
    (
        cert_no.map(str::to_string),
        vehicle_id.map(str::to_string),
        expires_on.map(str::to_string),
        matched_by.map(str::to_string),
    )
}
