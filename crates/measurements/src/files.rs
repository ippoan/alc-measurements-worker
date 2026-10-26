//! 顔写真・動画の口 (rust-alc-api の `alc-misc` の measurements.rs の `get_face_photo`・`get_video` の写し)。
//!
//! - `GET /measurements/{id}/face-photo` — R2 の顔写真を返す (`Content-Type: image/jpeg`)
//! - `GET /measurements/{id}/video` — R2 の動画を返す (`Content-Type: video/webm`)
//!
//! どちらも `Cache-Control: private, max-age=3600`、本文は R2 のバイト列。測定を `tenant_id` 付きで取る →
//! 無ければ 404、URL の列 (`face_photo_url` / `video_url`) が NULL でも 404 → URL から R2 のキーを取り出せなければ 500 →
//! R2 の取得に失敗 (オブジェクト無しを含む) しても 500。エラー応答の本文は空 (rust と同じ)。ログは固定の語だけ。
//! R2 の binding が無ければ (ローカルなど) この 2 口だけ 503 (ほかの口は動く)。
//!
//! DB は短い transaction で終えてから R2 を読む (R2 の future は `Send` でないので `tenant_tx` の中で await しない)。
//!
//! # キーの取り出しは rust と違う ([`extract_key`])
//!
//! rust は URL の接頭辞 `{R2_PUBLIC_URL_BASE}/` を剥いでキーにする。だがその既定値には account ID が入り、
//! public repo に書けない。保存時のキーは rust-alc-api の upload.rs で `{tenant_id}/{uuid}.jpg`・
//! `{tenant_id}/blow-video/{uuid}.webm` と決まっているので、この Worker は接頭辞を持たず、**要求の tenant_id で切り出す**。
//! 副作用として、別の tenant の写真・動画を指す URL は読めなくなる (rust より厳しい)。

use std::sync::Arc;

use alc_core_wasm::{DeviceDevSlot, TenantId};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use futures_util::future::BoxFuture;
use uuid::Uuid;

use crate::measurements::get_one;
use crate::{AppState, LogSink};

/// R2 の段の失敗。持つのは段の名前だけ (キーやランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError(pub &'static str);

/// ファイルの置き場 (Worker では R2 の binding `MEASUREMENTS_R2`、テストではモック)。
/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait FileStore: Send + Sync {
    /// 無いキーも失敗 (rust の `storage.download` と同じく 500 にする)
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>>;

    /// 置く ([`crate::upload`] の 3 口)
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;
}

pub type SharedStore = Arc<dyn FileStore>;

/// 口が返す 2 種類
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Media {
    FacePhoto,
    Video,
}

impl Media {
    /// ログの段の名前
    fn stage(self) -> &'static str {
        match self {
            Self::FacePhoto => "face-photo",
            Self::Video => "video",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::FacePhoto => "image/jpeg",
            Self::Video => "video/webm",
        }
    }

    /// キーの `{tenant_id}/` より後ろの (接頭辞, 拡張子)。間に uuid が入る
    /// (rust-alc-api の upload.rs: `{tenant_id}/{uuid}.jpg`・`{tenant_id}/blow-video/{uuid}.webm`)
    fn shape(self) -> (&'static str, &'static str) {
        match self {
            Self::FacePhoto => ("", ".jpg"),
            Self::Video => ("blow-video/", ".webm"),
        }
    }
}

/// 保存時の uuid の長さ (ハイフン込みの 36 字)
const UUID_LEN: usize = 36;

/// 保存した URL から R2 のキーを取り出す。URL の中で `/{tenant_id}/` が**最後に**現れる位置から後ろ (先頭の `/` は含めない)
/// をキーとし、`{tenant_id}/{uuid}.jpg` (動画は `{tenant_id}/blow-video/{uuid}.webm`) の形に一致するときだけ返す
/// (uuid は小文字の 16 進とハイフンの 36 字)。取り出せなければ `None` (= 呼び手は 500)。
/// 別の tenant の ID で始まる URL・接頭辞だけ・拡張子違い・`..` を含むものは `None`
pub fn extract_key(url: &str, tenant_id: Uuid, media: Media) -> Option<String> {
    let marker = format!("/{tenant_id}/");
    let key = &url[url.rfind(&marker)? + 1..];
    let (prefix, ext) = media.shape();
    let name = key
        .strip_prefix(&format!("{tenant_id}/"))?
        .strip_prefix(prefix)?
        .strip_suffix(ext)?;
    let valid = name.len() == UUID_LEN
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-');
    valid.then(|| key.to_owned())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/measurements/{id}/face-photo", get(get_face_photo))
        .route("/measurements/{id}/video", get(get_video))
}

async fn get_face_photo(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Response, StatusCode> {
    serve(state, tenant_id, dev, id, Media::FacePhoto).await
}

async fn get_video(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Response, StatusCode> {
    serve(state, tenant_id, dev, id, Media::Video).await
}

/// 保存した URL の写真・動画を R2 から読む (口 [`serve`] と検知メールの添付で同じ手順)。URL → [`extract_key`]
/// (要求の tenant_id で照合。別 tenant の URL は読めない) → [`FileStore::get`]。キーが取れない・R2 の失敗は
/// ログ 1 行 (`stage` と固定の語だけ。URL・キーを出さない) を残して `None`
pub async fn read_media(
    log: &LogSink,
    files: &SharedStore,
    tenant_id: Uuid,
    url: &str,
    media: Media,
    stage: &str,
) -> Option<Vec<u8>> {
    let Some(key) = extract_key(url, tenant_id, media) else {
        log(&format!("measurements: {stage} failed: key"));
        return None;
    };
    match files.get(&key).await {
        Ok(bytes) => Some(bytes),
        Err(e) => {
            log(&format!("measurements: {stage} failed: storage ({})", e.0));
            None
        }
    }
}

async fn serve(
    state: AppState,
    tenant_id: Uuid,
    dev: DeviceDevSlot,
    id: Uuid,
    media: Media,
) -> Result<Response, StatusCode> {
    let stage = media.stage();
    let Some(files) = state.files.clone() else {
        (state.log)(&format!(
            "measurements: {stage} failed: storage (unconfigured)"
        ));
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    // DB は短い transaction で終え、lock も離してから R2 を読む
    let measurement = {
        let mut pg = state.pg.lock().await;
        get_one(&mut pg, tenant_id, dev.get(), id)
            .await
            .map_err(|e| state.db_fail(stage, &e))?
    };
    let url = measurement
        .and_then(|m| match media {
            Media::FacePhoto => m.face_photo_url,
            Media::Video => m.video_url,
        })
        .ok_or(StatusCode::NOT_FOUND)?;
    let bytes = read_media(&state.log, &files, tenant_id, &url, media, stage)
        .await
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok((
        [
            (header::CONTENT_TYPE, media.content_type()),
            (header::CACHE_CONTROL, "private, max-age=3600"),
        ],
        bytes,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Uuid = Uuid::from_u128(0x1111_1111_2222_3333_4444_5555_5555_5555);

    /// 保存時と同じ形 (小文字の 16 進とハイフンの 36 字) の uuid
    fn u() -> String {
        Uuid::from_u128(0x0a1b_2c3d_4e5f_4a6b_8c7d_9e0f_1a2b_3c4d).to_string()
    }

    fn photo(url: &str) -> Option<String> {
        extract_key(url, T, Media::FacePhoto)
    }

    fn video(url: &str) -> Option<String> {
        extract_key(url, T, Media::Video)
    }

    #[test]
    fn extract_key_takes_the_tail_from_the_tenant_id() {
        let u = u();
        // 接頭辞 (ホスト・bucket の path) は何でもよい。要るのは `/{tenant}/` から後ろの形だけ
        assert_eq!(
            photo(&format!("https://files.example.test/{T}/{u}.jpg")),
            Some(format!("{T}/{u}.jpg"))
        );
        assert_eq!(
            video(&format!(
                "https://files.example.test/base/{T}/blow-video/{u}.webm"
            )),
            Some(format!("{T}/blow-video/{u}.webm"))
        );
    }

    #[test]
    fn extract_key_rejects_other_tenants_and_the_wrong_media() {
        let u = u();
        let other = Uuid::from_u128(1);
        assert_eq!(photo(&format!("https://h.test/{other}/{u}.jpg")), None);
        assert_eq!(
            video(&format!("https://h.test/{other}/blow-video/{u}.webm")),
            None
        );
        // 写真の URL を動画の口へ・動画の URL を写真の口へは通さない
        assert_eq!(video(&format!("https://h.test/{T}/{u}.jpg")), None);
        assert_eq!(
            photo(&format!("https://h.test/{T}/blow-video/{u}.webm")),
            None
        );
    }

    #[test]
    fn extract_key_rejects_malformed_tails() {
        let u = u();
        // tenant が URL に無い・接頭辞だけ
        assert_eq!(photo("https://h.test/x.jpg"), None);
        assert_eq!(photo(&format!("https://h.test/{T}/")), None);
        assert_eq!(video(&format!("https://h.test/{T}/blow-video/")), None);
        // 拡張子違い・uuid の長さ違い・大文字・query
        assert_eq!(photo(&format!("https://h.test/{T}/{u}.png")), None);
        assert_eq!(photo(&format!("https://h.test/{T}/{}.jpg", &u[1..])), None);
        assert_eq!(
            photo(&format!("https://h.test/{T}/{}.jpg", u.to_uppercase())),
            None
        );
        assert_eq!(photo(&format!("https://h.test/{T}/{u}.jpg?x=1")), None);
        // `..` を含む (36 字に収めても字種で落ちる)
        assert_eq!(photo(&format!("https://h.test/{T}/{}..jpg", &u[2..])), None);
        assert_eq!(photo(&format!("https://h.test/{T}/../{u}.jpg")), None);
    }

    #[test]
    fn extract_key_uses_the_last_occurrence_of_the_tenant_id() {
        let u = u();
        // tenant が 2 回出る URL は最後の位置から切る (手前の `/{tenant}/...` を鍵に混ぜない)
        assert_eq!(
            photo(&format!("https://h.test/{T}/dir/{T}/{u}.jpg")),
            Some(format!("{T}/{u}.jpg"))
        );
        // 最後の位置から後ろが形に合わなければ、手前が合っていても通さない
        assert_eq!(photo(&format!("https://h.test/{T}/{u}.jpg/{T}/x")), None);
    }
}
