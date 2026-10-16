//! 顔写真・動画の 2 口を口から実 DB + R2 のモックまで通す (rust-alc-api の tests/mock_tests/mock_measurements_test.rs の
//! `test_get_face_photo_*`・`test_get_video_*` の 12 件の写し。rust は mock の repo、ここは実 DB と `MockStore`)。
//!
//! 6 つの振る舞い (success・no_url・measurement_not_found・db_error・storage_download_error・extract_key_fails) を
//! 顔写真・動画の両方で、加えて別の tenant の URL (500)・R2 の binding 無し (503)。

mod support;

use measurements::files::SharedStore;
use serde_json::json;
use support::*;
use uuid::Uuid;

/// 2 口の違い (path・保存した列・Content-Type・キーの形)
struct Kind {
    sub: &'static str,
    column: &'static str,
    content_type: &'static str,
    stage: &'static str,
    /// `{uuid}` の前後 (`{tenant}/` より後ろ)
    prefix: &'static str,
    ext: &'static str,
}

const FACE: Kind = Kind {
    sub: "face-photo",
    column: "face_photo_url",
    content_type: "image/jpeg",
    stage: "face-photo",
    prefix: "",
    ext: ".jpg",
};

const VIDEO: Kind = Kind {
    sub: "video",
    column: "video_url",
    content_type: "video/webm",
    stage: "video",
    prefix: "blow-video/",
    ext: ".webm",
};

const BYTES: &[u8] = b"\xff\xd8\xffbytes-from-r2";

impl Kind {
    /// 保存時と同じ形のキー (rust-alc-api の upload.rs)
    fn key(&self, tenant: Uuid) -> String {
        format!("{tenant}/{}{}{}", self.prefix, Uuid::new_v4(), self.ext)
    }

    /// 保存した URL (公開 URL の接頭辞 + キー。接頭辞はこの Worker が見ない)
    fn url(key: &str) -> String {
        format!("https://files.example.test/{key}")
    }

    fn uri(&self, id: &str) -> String {
        format!("/api/measurements/{id}/{}", self.sub)
    }
}

/// 測定を保存し、`kind` の列に `url` を入れて id を返す
async fn measurement_with(app: &App, tenant: Uuid, kind: &Kind, url: Option<&str>) -> String {
    let emp = employee(tenant, "運行者", &Uuid::new_v4().simple().to_string()[..8]).await;
    let mut body = measurement_body(emp, "pass", "2026-09-12T01:00:00Z", false, None);
    if let Some(url) = url {
        body[kind.column] = json!(url);
    }
    let res = call(app, "POST", "/api/measurements", Some(tenant), Some(body)).await;
    assert_eq!(res.status, 201);
    res.json()["id"].as_str().unwrap().to_owned()
}

fn shared(store: &std::sync::Arc<MockStore>) -> SharedStore {
    store.clone()
}

async fn success(kind: Kind) {
    let tenant = tenant().await;
    let key = kind.key(tenant);
    let store = MockStore::with(&key, BYTES);
    let app = build_with(rt_client().await, Some(shared(&store)));
    let id = measurement_with(&app, tenant, &kind, Some(&Kind::url(&key))).await;

    let res = call(&app, "GET", &kind.uri(&id), Some(tenant), None).await;
    assert_eq!(res.status, 200);
    assert_eq!(res.headers["content-type"], kind.content_type);
    assert_eq!(res.headers["cache-control"], "private, max-age=3600");
    assert_eq!(res.body, BYTES);
    assert_eq!(store.gets(), [key], "R2 から読むキーは {{tenant}}/... の形");
    assert!(app.logs().is_empty());
}

async fn no_url(kind: Kind) {
    // URL の列が NULL → 404 (本文は空・R2 は読まない)
    let tenant = tenant().await;
    let store = MockStore::with("x", BYTES);
    let app = build_with(rt_client().await, Some(shared(&store)));
    let id = measurement_with(&app, tenant, &kind, None).await;

    let res = call(&app, "GET", &kind.uri(&id), Some(tenant), None).await;
    assert_eq!(res.status, 404);
    assert!(res.body.is_empty());
    assert!(store.gets().is_empty());
}

async fn measurement_not_found(kind: Kind) {
    let tenant = tenant().await;
    let store = MockStore::with("x", BYTES);
    let app = build_with(rt_client().await, Some(shared(&store)));

    let res = call(
        &app,
        "GET",
        &kind.uri(&Uuid::new_v4().to_string()),
        Some(tenant),
        None,
    )
    .await;
    assert_eq!(res.status, 404);
    assert!(res.body.is_empty());
    assert!(store.gets().is_empty());
}

async fn db_error(kind: Kind) {
    // DB に届かない (接続が切れた) → 500・本文は空・ログは段と kind だけ・R2 は読まない
    let tenant = tenant().await;
    let store = MockStore::with("x", BYTES);
    let app = build_with(broken_client().await, Some(shared(&store)));
    let id = Uuid::new_v4();

    let res = call(&app, "GET", &kind.uri(&id.to_string()), Some(tenant), None).await;
    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(
        logs[0].starts_with(&format!("measurements: {} failed: db (", kind.stage)),
        "{}",
        logs[0]
    );
    assert!(!logs[0].contains(&tenant.to_string()) && !logs[0].contains(&id.to_string()));
    assert!(store.gets().is_empty());
}

async fn storage_download_error(kind: Kind) {
    // R2 にオブジェクトが無い (取得の失敗) → 500・本文は空・ログに key も tenant も出ない
    let tenant = tenant().await;
    let key = kind.key(tenant);
    let store = MockStore::with("other-key", BYTES);
    let app = build_with(rt_client().await, Some(shared(&store)));
    let id = measurement_with(&app, tenant, &kind, Some(&Kind::url(&key))).await;

    let res = call(&app, "GET", &kind.uri(&id), Some(tenant), None).await;
    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    assert_eq!(store.gets(), std::slice::from_ref(&key));
    assert_eq!(
        app.logs(),
        [format!(
            "measurements: {} failed: storage (missing)",
            kind.stage
        )]
    );
}

async fn extract_key_fails(kind: Kind) {
    // 保存した URL から R2 のキーを取り出せない → 500・R2 は読まない・ログは固定
    let tenant = tenant().await;
    let store = MockStore::with("x", BYTES);
    let app = build_with(rt_client().await, Some(shared(&store)));
    let id = measurement_with(
        &app,
        tenant,
        &kind,
        Some("https://files.example.test/not-a-key"),
    )
    .await;

    let res = call(&app, "GET", &kind.uri(&id), Some(tenant), None).await;
    assert_eq!(res.status, 500);
    assert!(res.body.is_empty());
    assert!(store.gets().is_empty());
    assert_eq!(
        app.logs(),
        [format!("measurements: {} failed: key", kind.stage)]
    );
}

/// 1 つの振る舞いを顔写真・動画の 2 本の `#[tokio::test]` にする (rust の件名と同じ `test_get_face_photo_*`・`test_get_video_*`)
macro_rules! both {
    ($behavior:ident, $face:ident, $video:ident) => {
        #[tokio::test]
        async fn $face() {
            $behavior(FACE).await
        }

        #[tokio::test]
        async fn $video() {
            $behavior(VIDEO).await
        }
    };
}

both!(success, test_get_face_photo_success, test_get_video_success);
both!(no_url, test_get_face_photo_no_url, test_get_video_no_url);
both!(
    measurement_not_found,
    test_get_face_photo_measurement_not_found,
    test_get_video_measurement_not_found
);
both!(
    db_error,
    test_get_face_photo_db_error,
    test_get_video_db_error
);
both!(
    storage_download_error,
    test_get_face_photo_storage_download_error,
    test_get_video_storage_download_error
);
both!(
    extract_key_fails,
    test_get_face_photo_extract_key_fails,
    test_get_video_extract_key_fails
);

#[tokio::test]
async fn test_other_tenants_url_is_500_and_never_read() {
    // 別の tenant の写真・動画を指す URL (rust は読めたが、この Worker は要求の tenant で切り出すので読まない)
    let tenant_a = tenant().await;
    let tenant_b = tenant().await;
    for kind in [FACE, VIDEO] {
        let other_key = kind.key(tenant_b);
        let store = MockStore::with(&other_key, BYTES);
        let app = build_with(rt_client().await, Some(shared(&store)));
        let id = measurement_with(&app, tenant_a, &kind, Some(&Kind::url(&other_key))).await;

        let res = call(&app, "GET", &kind.uri(&id), Some(tenant_a), None).await;
        assert_eq!(res.status, 500, "{}", kind.sub);
        assert!(res.body.is_empty());
        assert!(
            store.gets().is_empty(),
            "別の tenant のキーを R2 に渡さない"
        );
        // 別の tenant からは測定そのものが見えない (404)
        let res = call(&app, "GET", &kind.uri(&id), Some(tenant_b), None).await;
        assert_eq!(res.status, 404, "{}", kind.sub);
    }
}

#[tokio::test]
async fn test_without_r2_binding_the_two_routes_are_503() {
    // R2 の binding が無い (ローカルなど) → 503。ほかの口は動く
    let app = build_with(rt_client().await, None);
    let tenant = tenant().await;
    for kind in [FACE, VIDEO] {
        let res = call(
            &app,
            "GET",
            &kind.uri(&Uuid::new_v4().to_string()),
            Some(tenant),
            None,
        )
        .await;
        assert_eq!(res.status, 503, "{}", kind.sub);
        assert!(res.body.is_empty());
    }
    assert_eq!(
        app.logs(),
        [
            "measurements: face-photo failed: storage (unconfigured)",
            "measurements: video failed: storage (unconfigured)"
        ]
    );
    let res = call(&app, "GET", "/api/measurements", Some(tenant), None).await;
    assert_eq!(res.status, 200);
}
