//! 点呼の記録の入口 (Refs ippoan/rust-alc-api#680)。点呼セッション (`tenko_sessions`) と点呼記録 (`tenko_records`) を
//! 書く worker はこの worker だけ (ippoan/alc-tenko-worker の段の口 `sessions.rs` から移した)。
//!
//! - [`record`] — 行の型・列の順・アルコールの結果の閉じ方・記録の hash と INSERT・内側の区切り (規則の唯一の置き場)
//! - [`normal`] — 通常点呼・IT点呼 (完了した測定から、同じ transaction でセッションと記録を 1 組作る)
//! - [`flow`] — 自動点呼・遠隔点呼の段 (`/tenko/sessions*`: 一覧・1 件・開始・医療・自己申告と安全判定・日常点検・携行品・
//!   指示の確認・運行報告・遠隔点呼への切り替え・中止・中断・再開・自己再開)
//! - [`alcohol`] — アルコールの段。測定の保存 (`PUT /measurements/{id}` の `tenko_session_id`) で、**測定の行の値から**進める
//!   (アルコールの結果を本文で受ける段の口は無い)
//! - [`judgment`] — 運行管理者の判定 (`POST /tenko/sessions/{id}/judgment`)。判定待ち (`judgment_pending`) を閉じて記録を書く
//! - [`fire`] — 点呼の節目の webhook を、応答の後に ippoan/alc-tenko-worker の内部の口へ渡す (送信はあちらの 1 か所)

pub mod alcohol;
pub mod fire;
pub mod flow;
pub mod judgment;
pub mod normal;
pub mod record;

pub use normal::*;
pub use record::{
    in_savepoint, record_payload, status_for_result, Savepoint, TenkoSession, SESSION_COLUMNS,
};

use axum::Router;

use crate::AppState;

/// 点呼のセッションの口 (`/tenko/sessions*`)
pub fn router() -> Router<AppState> {
    flow::router().merge(judgment::router())
}
