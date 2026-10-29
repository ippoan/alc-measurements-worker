//! 点呼の節目の webhook (アルコール検知・運行報告・中断・日常点検 NG) を、ippoan/alc-tenko-worker の内部の口
//! `POST /internal/webhooks/fire` へ渡す (Service Binding `ALC_TENKO`。Refs ippoan/rust-alc-api#680)。
//! 送信・署名・再送・配信の記録はあちらの `deliver.rs` の 1 か所 (予定超過の cron も同じ送り手) で、ここは渡すだけ。
//!
//! 口は commit の後に [`PendingFire`] を応答の extension に入れ、worker (`src/lib.rs`) が応答を返した後に
//! `ctx.wait_until` で [`PendingFire::send`] を走らせる (アルコール検知メールと同じ形)。
//! **dev端末の要求では作らない** (dev の段の行が本番の送り先に届かないように。あちらの口も dev は積まない)。
//! 渡せなくても応答は変えない・再送しない (ログ 1 行。種類と段の label だけ)。

use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::service::{ServiceRequest, SharedService};
use crate::{AppState, LogSink};

/// あちらの口 (`/api` の外。auth-worker からは届かない)
pub const FIRE_PATH: &str = "/internal/webhooks/fire";

/// 渡す 1 件 (種類と本文。本文の `tenant_id` はテナントのヘッダーと同じでなければあちらが 400 で弾く)
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event_type: &'static str,
    pub payload: Value,
}

impl alc_worker_db::TxOutput for Event {}

impl Event {
    pub fn new(event_type: &'static str, payload: Value) -> Self {
        Self {
            event_type,
            payload,
        }
    }
}

/// 応答の後に渡す控え (DB は持たない)
#[derive(Clone)]
pub struct PendingFire {
    service: SharedService,
    tenant_id: Uuid,
    events: Vec<Event>,
    log: LogSink,
}

impl PendingFire {
    /// 渡すものが無い・dev端末の要求・binding が無い (`ALC_TENKO` を持たない環境) なら `None`
    pub fn new(state: &AppState, tenant_id: Uuid, dev: bool, events: Vec<Event>) -> Option<Self> {
        if dev || events.is_empty() {
            return None;
        }
        let Some(service) = state.tenko.clone() else {
            (state.log)("measurements: tenko webhook skipped: no binding");
            return None;
        };
        Some(Self {
            service,
            tenant_id,
            events,
            log: Arc::clone(&state.log),
        })
    }

    /// 1 件ずつ渡す。届かない・202 でなければログ 1 行 (種類と label・status だけ)
    pub async fn send(self) {
        for e in self.events {
            let req = ServiceRequest {
                path: FIRE_PATH.to_owned(),
                headers: vec![
                    ("content-type", "application/json".to_owned()),
                    ("x-tenant-id", self.tenant_id.to_string()),
                ],
                body: json!({ "event_type": e.event_type, "payload": e.payload }).to_string(),
            };
            match self.service.post(req).await {
                Ok(r) if r.status == 202 => {}
                Ok(r) => (self.log)(&format!(
                    "measurements: tenko webhook {} refused: status {}",
                    e.event_type, r.status
                )),
                Err(u) => (self.log)(&format!(
                    "measurements: tenko webhook {} unreachable: {}",
                    e.event_type, u.0
                )),
            }
        }
    }
}
