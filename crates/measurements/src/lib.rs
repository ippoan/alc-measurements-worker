//! 測定の口 (Refs ippoan/alc-app#436、ippoan/rust-alc-api#697)。rust-alc-api の `crates/alc-misc` の測定の口を移す先で、
//! path・method・JSON の形・status は rust と同じ (運行者端末・管理画面がそのまま使う)。
//!
//! - [`measurements`] — `/api/measurements` の 5 口 (開始・更新・作成・一覧・取得)。rust の `alc-misc` の
//!   measurements.rs と repo/measurements.rs の写し
//! - [`models`] — 入出力の形 (rust の `alc_core::models` の Measurement 系の写し。serde の rename / alias も同じ)
//! - [`tenko`] — 通常点呼の記録 (完了した測定から点呼セッションと点呼記録を作る副作用)。rust の `alc-tenko` の
//!   normal_tenko.rs と、tenko_sessions.rs・repo/tenko_sessions.rs の共有部分の写し
//! - [`carins`] — 電子車検証の番号の検査と、車検期限の照合 (rust の `alc-core` の car_inspections の写し)
//!
//! - [`files`] — 顔写真・動画の口 (`/api/measurements/{id}/face-photo`・`/video`。R2 から読んで返す。置き場は
//!   [`files::FileStore`] で差し替える)
//! - [`deferred`] — 点呼の記録の確定を後回しにした測定の確定 (`POST /api/measurements/{id}/tenko` と、worker の定時実行の
//!   見回り [`deferred::sweep`])。rust に無い
//! - [`upload`] — 顔写真・吹きかけ動画・運行報告の音声を R2 に置く口 (`/api/upload/{face-photo,blow-video,report-audio}`)。
//!   rust の `alc-misc` の upload.rs の写し (Refs ippoan/rust-alc-api#680)
//! - [`bp_settings`] — 血圧の通知の設定 (`/api/bp-notify/...` の 4 口。admin だけ。本人への LINE WORKS と日報メールの宛先を
//!   専用の表 `bp_notify_settings` で持つ)
//! - [`alert`] — アルコール検知メール (測定のアルコール値が 0 より上になった保存で、notify のグループ「アルコール検知」の
//!   メンバーへ 1 人 1 通。送り手は [`alert::AlertMailer`] で差し替える)
//!
//! 全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。`alc_core_wasm::require_tenant_header`)。
//! rust と同じく role は見ない (例外は [`alert::alert_test`]・[`report::bp_report_test`] のテスト送信と血圧の通知の設定の口で、`X-User-Role` が admin でなければ 403)。dev端末の印 `X-Device-Dev` は rust と同じく接続の設定 `app.device_dev` に通す
//! (`require_tenant_header` が認証の後に書く `DeviceDevSlot` を口が読み、[`set_device_dev`] で立てる)。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。
//! ログ・エラー応答は固定の語だけ (エラーの文・識別子・入力値を載せない。DB のエラーは `kind` = SQLSTATE か固定の label)。

pub mod alert;
pub mod bp;
pub mod bp_settings;
pub mod carins;
pub mod deferred;
pub mod files;
pub mod measurements;
pub mod models;
pub mod report;
pub mod service;
pub mod tenko;
pub mod upload;

use std::sync::Arc;

use alc_core_wasm::{require_tenant_header, DeviceDevSlot};
use alc_worker_db::{kind, PgClient, TenantTx};
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use axum::{middleware, Router};
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

/// 今の時刻 (UTC)。chrono を時計なしで使うので、Worker では `Date.now()`、テストでは固定の時刻を渡す
pub type NowFn = Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
    /// 顔写真・動画の置き場 (R2)。無ければ (binding が無い) その 2 口だけ 503
    pub files: Option<files::SharedStore>,
    /// アルコール検知メールの送り手 (`send_email` の binding)。無ければ (binding か差出人が無い) メールは送らない
    pub mailer: Option<alert::SharedMailer>,
    /// LINE WORKS の送り先 (ippoan/alc-lineworks-worker の Service Binding `ALC_LINEWORKS`)。無ければ血圧の本人通知は送らない
    pub lineworks: Option<service::SharedService>,
    /// 今の時刻 (血圧の日報のテスト送信が「前日」を決めるのに使う)
    pub now: NowFn,
}

impl AppState {
    /// DB の失敗を固定の語でログに出し、500 にする
    pub(crate) fn db_fail(&self, stage: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("measurements: {stage} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// dev端末の要求で、transaction の中だけ `app.device_dev` を `'1'` にする文 (固定。値を埋め込まない)。
/// rust の `alc_core::tenant::set_current_tenant` と同じ働き (rust は接続ごと、ここは transaction-local)。
/// 列 `is_dev` の既定値と RLS がこれを見て、dev の行と本番の行を分ける (alc-migrations 153)
pub const SET_DEVICE_DEV: &str = "SELECT set_config('app.device_dev', '1', true)";

/// `dev` なら [`SET_DEVICE_DEV`] を流す (tenant_tx の最初の文。dev でなければ何もしない = 設定は空のまま)
pub async fn set_device_dev(tx: &TenantTx<'_>, dev: bool) -> Result<(), tokio_postgres::Error> {
    if dev {
        tx.execute_typed(SET_DEVICE_DEV, &[]).await?;
    }
    Ok(())
}

/// 要求ごとに空の `DeviceDevSlot` を置く (kit の `require_tenant_header` は、在る slot にだけ認証の後で dev の印を書く。
/// rust の `require_tenant_header` が要求ごとに slot を作るのと同じ。共有の 1 つを使い回さない)
pub async fn with_device_dev_slot(mut req: Request, next: Next) -> Response {
    req.extensions_mut().insert(DeviceDevSlot::default());
    next.run(req).await
}

/// 本文の上限 (rust の main.rs の `DefaultBodyLimit` と同じ 20MB)
pub const BODY_LIMIT: usize = 20 * 1024 * 1024;

/// 全部の口。rust と同じく `/api` の下 (auth-worker は `/api/measurements...` のまま Service Binding に渡す)
pub fn router() -> Router<AppState> {
    // 後に足した route_layer が外側: slot を置く → tenant と dev の印を読む → 口
    let api = measurements::router()
        .merge(files::router())
        .merge(upload::router())
        .merge(bp_settings::router())
        .route_layer(middleware::from_fn(require_tenant_header))
        .route_layer(middleware::from_fn(with_device_dev_slot));
    Router::new()
        .nest("/api", api)
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}
