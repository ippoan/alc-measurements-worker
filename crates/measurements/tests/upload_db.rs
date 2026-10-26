//! upload の 3 口 (顔写真・吹きかけ動画・運行報告の音声) を口から実 DB (uuid) + R2 のモックまで通す
//! (rust-alc-api の `alc-misc` の upload.rs の写し。Refs ippoan/rust-alc-api#680)。
//! 置いたものを顔写真・動画の口で読み返し、返した `url` が読めることも確かめる。

mod support;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use measurements::files::SharedStore;
use measurements::upload::URL_PREFIX;
use serde_json::json;
use support::*;
use uuid::Uuid;

const BOUNDARY: &str = "upload-test-boundary";

fn multipart_type() -> String {
    format!("multipart/form-data; boundary={BOUNDARY}")
}

fn multipart(data: &[u8]) -> Vec<u8> {
    let mut body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x.bin\"\r\nContent-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn upload(app: &App, tenant: Uuid, sub: &str, body: Vec<u8>) -> Got {
    let ct = multipart_type();
    call_raw(
        app,
        "POST",
        &format!("/api/upload/{sub}"),
        Some(tenant),
        &[("content-type", ct.as_str())],
        body,
    )
    .await
}

#[tokio::test]
async fn uploads_put_the_rust_keys_and_return_readable_urls() {
    let store = Arc::new(MockStore::default());
    let app = build_with(rt_client().await, Some(store.clone() as SharedStore));
    let t = tenant().await;

    for (sub, prefix, ext, ct) in [
        ("face-photo", "", ".jpg", "image/jpeg"),
        ("blow-video", "blow-video/", ".webm", "video/webm"),
        ("report-audio", "report-audio/", ".webm", "audio/webm"),
    ] {
        let got = upload(&app, t, sub, multipart(b"media-bytes")).await;
        assert_eq!(got.status, 200, "{sub}");
        let v: serde_json::Value = serde_json::from_slice(&got.body).unwrap();
        let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        keys.sort();
        assert_eq!(keys, ["filename", "url"]);
        let filename = v["filename"].as_str().unwrap();
        let file_uuid = filename.strip_suffix(ext).unwrap();
        assert!(Uuid::parse_str(file_uuid).is_ok());
        let key = format!("{t}/{prefix}{filename}");
        assert_eq!(v["url"], format!("{URL_PREFIX}{key}"));
        assert_eq!(store.puts().last().unwrap(), &(key.clone(), ct.to_owned()));
        assert_eq!(store.objects.lock().unwrap()[&key], b"media-bytes");
    }

    // 返した url を測定に載せると、顔写真・動画の口で読める
    let urls: Vec<String> = store
        .puts()
        .iter()
        .take(2)
        .map(|(k, _)| format!("{URL_PREFIX}{k}"))
        .collect();
    let emp = employee(t, "upload", "U1").await;
    let id = start_measurement(&app, t, emp).await;
    put_measurement_ok(
        &app,
        t,
        &id,
        &json!({"face_photo_url": urls[0], "video_url": urls[1]}),
    )
    .await;
    for sub in ["face-photo", "video"] {
        let got = call(
            &app,
            "GET",
            &format!("/api/measurements/{id}/{sub}"),
            Some(t),
            None,
        )
        .await;
        assert_eq!(
            (got.status, got.body.as_slice()),
            (200, b"media-bytes".as_slice()),
            "{sub}"
        );
    }
    assert!(app.logs().is_empty(), "{:?}", app.logs());
}

#[tokio::test]
async fn bad_multipart_missing_store_and_failures() {
    let t = tenant().await;

    // R2 の binding が無い → 503 (multipart は読まない)
    let app = build_with(rt_client().await, None);
    assert_eq!(
        upload(&app, t, "face-photo", b"garbage".to_vec())
            .await
            .status,
        503
    );
    assert_eq!(
        app.logs(),
        ["measurements: upload face-photo failed: storage (unconfigured)"]
    );

    let store = Arc::new(MockStore::default());
    let app = build_with(rt_client().await, Some(store.clone() as SharedStore));
    // multipart が読めない・空 → 400
    assert_eq!(
        upload(&app, t, "blow-video", b"garbage".to_vec())
            .await
            .status,
        400
    );
    assert_eq!(
        upload(
            &app,
            t,
            "blow-video",
            format!("--{BOUNDARY}--\r\n").into_bytes()
        )
        .await
        .status,
        400
    );
    // R2 に置けない → 500
    store.fail_put.store(true, Ordering::SeqCst);
    assert_eq!(
        upload(&app, t, "report-audio", multipart(b"a"))
            .await
            .status,
        500
    );
    assert_eq!(
        app.logs(),
        ["measurements: upload report-audio failed: storage (put)"]
    );
    // tenant が無い → 401
    let got = call_raw(
        &app,
        "POST",
        "/api/upload/face-photo",
        None,
        &[],
        multipart(b"a"),
    )
    .await;
    assert_eq!(got.status, 401);

    // DB に繋がらない (uuid が取れない) → 500。R2 には置かない
    let store = Arc::new(MockStore::default());
    let app = build_with(broken_client().await, Some(store.clone() as SharedStore));
    assert_eq!(
        upload(&app, t, "face-photo", multipart(b"a")).await.status,
        500
    );
    assert!(store.puts().is_empty());
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("measurements: upload face-photo failed: db ("));
}
