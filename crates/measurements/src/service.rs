//! 別の Worker への Service Binding の 1 リクエスト (ippoan/alc-trouble-worker の `service.rs` と同じ型。Refs ippoan/alc-app#436)。
//!
//! この Worker が呼ぶのは LINE WORKS の ippoan/alc-lineworks-worker (`ALC_LINEWORKS`) だけ: 送信 ([`crate::bp`]) と
//! メンバー一覧 ([`crate::bp_settings`])。
//! 口の側は [`Service`] を呼ぶだけで、Worker の実装は直下の `src/service.rs` (Fetcher)、テストはモックに差し替える。
//!
//! **await を `tenant_tx` の閉包の中に入れない** (DB の transaction を他の Worker の応答待ちで延ばさない)。

use std::sync::Arc;

use futures_util::future::BoxFuture;

/// 1 リクエスト。`path` は path と query (host は binding が決めるので持たない)。`body` が在れば JSON で送る
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRequest {
    pub path: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

/// 応答の status と本文 (送信は status だけ読む。メンバー一覧は本文を読む)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// 相手に届かなかった (fetch の失敗)。中身はログに出す段の label
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unreachable(pub &'static str);

/// Service Binding への POST / GET (axum の state と handler が要るので `Send + Sync` で `Send` な future を返す)。
/// GET は `body` を送らない
pub trait Service: Send + Sync {
    fn post(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>>;
    fn get(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>>;
}

pub type SharedService = Arc<dyn Service>;
