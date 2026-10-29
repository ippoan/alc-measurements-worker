//! 測定の Worker `alc-measurements` (Refs ippoan/alc-app#436、ippoan/rust-alc-api#697)。crates/measurements の口
//! (`router`、axum) を workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-trouble-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! アルコール検知メールは、口が応答の extension に入れた [`PendingAlert`] を取り出し、応答の後 (`wait_until`) に送る。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。
//!
//! 定時実行 (`scheduled`、wrangler.toml の `[triggers]`。1 分ごと) は、点呼の確定を後回しにしたまま 2 分を過ぎた測定を
//! `通常点呼` で確定する見回り ([`measurements::deferred::sweep`]。Refs ippoan/alc-app#436)。ログは件数だけ。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/measurements だけを検査させる
#![cfg(target_arch = "wasm32")]

mod db;
mod mail;
mod r2;
mod service;
mod tcp;

use std::sync::Arc;

use crate::mail::EmailSender;
use crate::r2::R2Store;
use crate::service::ServiceBinding;
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use futures_util::lock::Mutex;
use measurements::alert::PendingAlert;
use measurements::bp::PendingBp;
use measurements::tenko::fire::PendingFire;
use measurements::{router, AppState, LogSink};
use tower_service::Service;
use worker::{
    console_error, console_log, event, Context, Env, HttpRequest, Result, ScheduleContext,
    ScheduledEvent, WorkerVersionMetadata,
};

/// 顔写真・動画の R2 の binding (wrangler.toml の `[[r2_buckets]]`。本番と staging で別の bucket)。
/// 無ければ顔写真・動画の口だけが 503 (ほかの口は動く)
const MEASUREMENTS_R2_BINDING: &str = "MEASUREMENTS_R2";

/// アルコール検知メールの `send_email` の binding (wrangler.toml の `[[send_email]]`。トップと staging の両方に在る) と、
/// 差出人の変数。どちらかが無ければメールは送らない (ほかの口は動く)
const ALERT_EMAIL_BINDING: &str = "ALERT_EMAIL";
const NOTIFY_EMAIL_FROM_VAR: &str = "NOTIFY_EMAIL_FROM";

/// alc-lineworks-worker の Service Binding (wrangler.toml の `[[services]]`。血圧の本人への LINE WORKS)。無ければ送らない
const ALC_LINEWORKS_BINDING: &str = "ALC_LINEWORKS";

/// alc-tenko-worker の Service Binding (wrangler.toml の `[[services]]`。点呼の webhook の渡し先)。無ければ渡さない
const ALC_TENKO_BINDING: &str = "ALC_TENKO";

/// 送り手を作る。binding が無い・差出人が未設定なら `None`
fn alert_mailer(env: &Env) -> Option<measurements::alert::SharedMailer> {
    let binding = env.send_email(ALERT_EMAIL_BINDING).ok()?;
    let from = env.var(NOTIFY_EMAIL_FROM_VAR).ok()?.to_string();
    if from.is_empty() {
        return None;
    }
    Some(Arc::new(EmailSender::new(binding, from)))
}

fn log_sink() -> LogSink {
    Arc::new(|message| console_error!("{message}"))
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, ctx: Context) -> Result<Response<Body>> {
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-trouble-worker と同じ)
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("measurements: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("measurements: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: log_sink(),
        files: env
            .bucket(MEASUREMENTS_R2_BINDING)
            .ok()
            .map(|b| Arc::new(R2Store::new(b)) as measurements::files::SharedStore),
        mailer: alert_mailer(&env),
        lineworks: env
            .service(ALC_LINEWORKS_BINDING)
            .ok()
            .map(|f| Arc::new(ServiceBinding::new(f)) as measurements::service::SharedService),
        tenko: env
            .service(ALC_TENKO_BINDING)
            .ok()
            .map(|f| Arc::new(ServiceBinding::new(f)) as measurements::service::SharedService),
        now: Arc::new(|| {
            measurements::report::from_millis(worker::Date::now().as_millis() as f64)
                .unwrap_or_default()
        }),
    };
    let mut resp = match router().with_state(state).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // アルコール検知メール (顔写真の読みと送信) は応答を返した後に回す。応答の本文・ヘッダーは変えない
    if let Some(pending) = resp.extensions_mut().remove::<PendingAlert>() {
        ctx.wait_until(async move {
            let _ = pending.send().await;
        });
    }
    // 血圧の本人への LINE WORKS も、応答を返した後に送る
    if let Some(pending) = resp.extensions_mut().remove::<PendingBp>() {
        ctx.wait_until(async move {
            let _ = pending.send().await;
        });
    }
    // 点呼の webhook も、応答を返した後に alc-tenko-worker へ渡す
    if let Some(pending) = resp.extensions_mut().remove::<PendingFire>() {
        ctx.wait_until(async move {
            pending.send().await;
        });
    }
    // どの版が応えたかを応答ヘッダーで分かるようにする。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}

/// 定時実行。本番の `0 22 * * *` (日本時間 7:00) は前日の血圧の日報メール、それ以外 (1 分ごと) は見回り
/// (確定待ちのまま 2 分を過ぎた測定を `通常点呼` で確定する)。DB の経路は fetch と同じ [`db::connect`]
#[event(scheduled)]
async fn scheduled(event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    let mut pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(e) => {
            console_error!("measurements: scheduled {e}");
            return;
        }
    };
    if event.cron() == measurements::report::DAILY_CRON {
        let Some(now) = measurements::report::from_millis(event.schedule()) else {
            console_error!("measurements: bp report skipped: no time");
            return;
        };
        if let Some(summary) =
            measurements::report::run(&mut pg, alert_mailer(&env), &log_sink(), now).await
        {
            console_log!("{}", summary.line());
        }
        return;
    }
    if let Some(summary) = measurements::deferred::sweep(&mut pg, &log_sink()).await {
        console_log!("{}", summary.line());
    }
}
