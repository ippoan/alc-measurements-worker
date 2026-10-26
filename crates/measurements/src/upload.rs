//! 顔写真・吹きかけ動画・運行報告の音声を R2 に置く口 (rust-alc-api の `alc-misc` の upload.rs の写し。Refs ippoan/rust-alc-api#680)。
//!
//! - `POST /upload/face-photo` — `{tenant_id}/{uuid}.jpg` に `image/jpeg` で置く
//! - `POST /upload/blow-video` — `{tenant_id}/blow-video/{uuid}.webm` に `video/webm` で置く
//! - `POST /upload/report-audio` — `{tenant_id}/report-audio/{uuid}.webm` に `audio/webm` で置く
//!
//! どれも multipart の最初の 1 つを読み (名前・Content-Type は見ない。rust と同じ)、`{url, filename}` を返す (200)。
//! キーと Content-Type は rust と同じ。R2 の binding が無ければ 503、multipart が読めない・空なら 400、R2 の失敗は 500
//! (エラー応答の本文は空)。上限は rust と同じ 20MB ([`crate::router`] の `DefaultBodyLimit`。超えると multipart の読みが失敗して 400)。
//!
//! # `url` は rust と違う ([`media_url`])
//!
//! rust は `{R2_PUBLIC_URL_BASE}/{key}` を返す。その既定値には account ID が入り public repo に書けず、bucket の公開も
//! 無効 (URL を開いても読めない)。`url` を読むのは顔写真・動画の口 ([`crate::files::extract_key`]) だけで、URL の中の
//! `/{tenant_id}/` から後ろをキーにする。なのでこの Worker は接頭辞を [`URL_PREFIX`] に固定して `{URL_PREFIX}{key}` を返す
//! (rust の行と同じ取り出し方で読める)。
//!
//! 新しいファイルの uuid は `gen_random_uuid()` で取る (wasm で乱数の crate を持たないため。tenant の transaction は
//! R2 の await の前に閉じる)。ログは固定の語だけ (キー・tenant を出さない)。

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::Serialize;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::AppState;

/// 返す `url` の接頭辞 (読み手はここを見ない。モジュールの注)
pub const URL_PREFIX: &str = "r2://alc-media/";

/// 3 口の違い
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    FacePhoto,
    BlowVideo,
    ReportAudio,
}

impl Kind {
    /// `{tenant_id}/` より後ろの (接頭辞, 拡張子)
    fn shape(self) -> (&'static str, &'static str) {
        match self {
            Self::FacePhoto => ("", "jpg"),
            Self::BlowVideo => ("blow-video/", "webm"),
            Self::ReportAudio => ("report-audio/", "webm"),
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::FacePhoto => "image/jpeg",
            Self::BlowVideo => "video/webm",
            Self::ReportAudio => "audio/webm",
        }
    }

    fn stage(self) -> &'static str {
        match self {
            Self::FacePhoto => "upload face-photo",
            Self::BlowVideo => "upload blow-video",
            Self::ReportAudio => "upload report-audio",
        }
    }
}

/// rust の `UploadResponse`
#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub url: String,
    pub filename: String,
}

/// (R2 のキー, ファイル名)。rust と同じ形
pub fn object_key(tenant_id: Uuid, kind: Kind, file_uuid: Uuid) -> (String, String) {
    let (prefix, ext) = kind.shape();
    let filename = format!("{file_uuid}.{ext}");
    (format!("{tenant_id}/{prefix}{filename}"), filename)
}

/// 返す `url`
pub fn media_url(key: &str) -> String {
    format!("{URL_PREFIX}{key}")
}

/// 新しいファイルの uuid (`gen_random_uuid()`)
pub async fn new_uuid(pg: &mut PgClient, tenant_id: Uuid) -> Result<Uuid, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            Ok(tx
                .query_typed_one("SELECT gen_random_uuid()", &[])
                .await?
                .get(0))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/upload/face-photo", post(face_photo))
        .route("/upload/blow-video", post(blow_video))
        .route("/upload/report-audio", post(report_audio))
}

async fn face_photo(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    multipart: Multipart,
) -> Result<Json<UploadResponse>, StatusCode> {
    upload(state, tenant_id, Kind::FacePhoto, multipart).await
}

async fn blow_video(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    multipart: Multipart,
) -> Result<Json<UploadResponse>, StatusCode> {
    upload(state, tenant_id, Kind::BlowVideo, multipart).await
}

async fn report_audio(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    multipart: Multipart,
) -> Result<Json<UploadResponse>, StatusCode> {
    upload(state, tenant_id, Kind::ReportAudio, multipart).await
}

/// R2 の binding (503) → multipart の読み (400) → uuid → R2 に置く
async fn upload(
    state: AppState,
    tenant_id: Uuid,
    kind: Kind,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, StatusCode> {
    let stage = kind.stage();
    let Some(files) = state.files.clone() else {
        (state.log)(&format!(
            "measurements: {stage} failed: storage (unconfigured)"
        ));
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    let field = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
    let file_uuid = {
        let mut pg = state.pg.lock().await;
        new_uuid(&mut pg, tenant_id).await
    }
    .map_err(|e| state.db_fail(stage, &e))?;
    let (key, filename) = object_key(tenant_id, kind, file_uuid);
    if let Err(e) = files.put(&key, data.to_vec(), kind.content_type()).await {
        (state.log)(&format!("measurements: {stage} failed: storage ({})", e.0));
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok(Json(UploadResponse {
        url: media_url(&key),
        filename,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::{extract_key, Media};

    const T: Uuid = Uuid::from_u128(0x1111_1111_2222_3333_4444_5555_5555_5555);
    const F: Uuid = Uuid::from_u128(0x0a1b_2c3d_4e5f_4a6b_8c7d_9e0f_1a2b_3c4d);

    #[test]
    fn keys_and_types_are_the_rust_layout() {
        assert_eq!(
            object_key(T, Kind::FacePhoto, F),
            (format!("{T}/{F}.jpg"), format!("{F}.jpg"))
        );
        assert_eq!(
            object_key(T, Kind::BlowVideo, F),
            (format!("{T}/blow-video/{F}.webm"), format!("{F}.webm"))
        );
        assert_eq!(
            object_key(T, Kind::ReportAudio, F),
            (format!("{T}/report-audio/{F}.webm"), format!("{F}.webm"))
        );
        assert_eq!(Kind::FacePhoto.content_type(), "image/jpeg");
        assert_eq!(Kind::BlowVideo.content_type(), "video/webm");
        assert_eq!(Kind::ReportAudio.content_type(), "audio/webm");
    }

    #[test]
    fn returned_urls_are_readable_by_the_media_routes() {
        let (key, _) = object_key(T, Kind::FacePhoto, F);
        assert_eq!(
            extract_key(&media_url(&key), T, Media::FacePhoto),
            Some(key)
        );
        let (key, _) = object_key(T, Kind::BlowVideo, F);
        assert_eq!(extract_key(&media_url(&key), T, Media::Video), Some(key));
    }
}
