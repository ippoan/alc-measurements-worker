//! 点呼の記録の確定を後回しにした測定 (`PUT` の `defer_tenko`) を確定する。rust には無い (Refs ippoan/alc-app#436)。
//!
//! 運行者端末は測定の結果が出た瞬間に保存し (`defer_tenko: true`)、点呼の記録の入力を測定の列 `tenko_pending` に置く。
//! 記録 (`tenko_records` は UPDATE / DELETE できず、点呼方法と hash が入る) は次のどちらかで 1 回だけ作る:
//!
//! - [`confirm_tenko`] — `POST /measurements/{id}/tenko` (`{"tenko_method": "通常点呼" | "IT点呼"}`)。結果カードの
//!   「IT点呼」「終了」
//! - [`sweep`] — 見回り (worker の定時実行。1 分ごと)。完了から 2 分 ([`sql::LIST_PENDING`]) を過ぎても確定待ちの測定を `通常点呼` で確定する
//!
//! 確定は `tenko_pending` を NULL にする UPDATE で入力を 1 回だけ取り ([`sql::CLAIM`])、同じ transaction で
//! 既存の記録の作り方 ([`crate::tenko::record_or_existing_session_id`]。列・hash は変えない) を呼ぶ。記録に失敗したら
//! transaction ごと戻す (入力は残り、見回りがまた拾う)。取れなかった (確定済み) ときは、既にある session の点呼方法と
//! 頼まれた方法を比べる (同じなら同じ id、違えば 409)。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{kind, PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::measurements::VALID_TENKO_METHODS;
use crate::models::Measurement;
use crate::tenko::{existing_session, record_or_existing_session_id, NormalTenkoInput};
use crate::{set_device_dev, AppState, LogSink};

/// 見回りが確定に使う点呼方法
pub const SWEEP_METHOD: &str = crate::tenko::TENKO_METHOD_NORMAL;

pub mod sql {
    use crate::models::MEASUREMENT_COLUMNS;

    /// 測定の status と確定待ちの入力 (`FOR UPDATE`。確定の口と見回りが同じ測定を同時に確定しようとしても 1 つずつ通す。
    /// 入力はここで読む — 下の [`claim`] の RETURNING は UPDATE の後の値 (NULL) しか返さない)
    pub const STATUS: &str = "SELECT status, tenko_pending FROM measurements \
         WHERE id = $1 AND tenant_id = $2 FOR UPDATE";

    /// 確定待ちの入力を 1 回だけ取る (NULL にする)。確定待ちだった (= 取れた) ときだけ測定の行を返す
    pub fn claim() -> String {
        format!(
            "UPDATE measurements SET tenko_pending = NULL \
             WHERE id = $1 AND tenant_id = $2 AND tenko_pending IS NOT NULL \
             RETURNING {MEASUREMENT_COLUMNS}"
        )
    }

    /// テナントをまたいだ確定待ちの一覧 (SECURITY DEFINER の関数。alc-migrations 180)。
    /// 完了から 2 分を過ぎたものだけ (待ちは固定。入力から作らない)
    pub const LIST_PENDING: &str =
        "SELECT tenant_id, measurement_id, is_dev FROM list_pending_tenko(interval '2 minutes')";
}

/// 確定の結果
#[derive(Debug, PartialEq)]
pub enum Confirmed {
    /// 測定が無い (別テナント・dev の軸の違いを含む) → 404
    NotFound,
    /// 頼まれた方法の点呼セッションがある (今回作った、または同じ方法で確定済み) → 200
    Recorded(Uuid),
    /// 確定できない → 409。`tenko_method` は既にある session の方法 (無ければ `None`)
    Conflict {
        error: &'static str,
        tenko_method: Option<String>,
    },
}

impl TxOutput for Confirmed {}

/// 測定 `id` の点呼の記録を `tenko_method` で確定する (tenant_tx + dev の印。確定の口と見回りが同じものを使う)
pub async fn confirm(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    tenko_method: String,
    log: LogSink,
) -> Result<Confirmed, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let Some(status) = tx
                .query_typed_opt(sql::STATUS, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?
            else {
                return Ok(Confirmed::NotFound);
            };
            let pending: Option<serde_json::Value> = status.get(1);
            if status.get::<_, String>(0) != "completed" {
                return Ok(Confirmed::Conflict {
                    error: "measurement_not_completed",
                    tenko_method: None,
                });
            }
            let claimed = tx
                .query_typed_opt(
                    &sql::claim(),
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            if let Some(row) = claimed {
                let m = Measurement::from_row(&row);
                // 入力は PUT が書いた JSON (行は FOR UPDATE で押さえてあるので、取った値と同じ)。
                // 読めない (手で書き換えた) ときは既定 (種別 normal・照合なし) で記録する
                let mut input: NormalTenkoInput = pending
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default();
                input.tenko_method = Some(tenko_method.clone());
                record_or_existing_session_id(tx, &m, &input, &log).await?;
            }
            Ok(match existing_session(tx, tenant_id, id).await? {
                Some((session_id, method)) if method == tenko_method => {
                    Confirmed::Recorded(session_id)
                }
                Some((_, method)) => Confirmed::Conflict {
                    error: "tenko_method_conflict",
                    tenko_method: Some(method),
                },
                None => Confirmed::Conflict {
                    error: "no_pending_tenko",
                    tenko_method: None,
                },
            })
        })
    })
    .await
}

/// `POST /measurements/{id}/tenko` の本文
#[derive(Debug, Deserialize)]
pub struct ConfirmTenko {
    #[serde(default)]
    pub tenko_method: Option<String>,
}

/// 点呼の確定 — 200 `{"tenko_session_id", "tenko_method"}` / 409 `{"error", "tenko_method"}` / 404 / 400 (方法が 2 値以外)
pub async fn confirm_tenko(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<ConfirmTenko>,
) -> Result<Response, StatusCode> {
    let method = body
        .tenko_method
        .filter(|m| VALID_TENKO_METHODS.contains(&m.as_str()))
        .ok_or(StatusCode::BAD_REQUEST)?;
    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let confirmed = confirm(
        &mut pg,
        tenant_id,
        dev,
        id,
        method.clone(),
        state.log.clone(),
    )
    .await
    .map_err(|e| state.db_fail("tenko confirm", &e))?;
    match confirmed {
        Confirmed::NotFound => Err(StatusCode::NOT_FOUND),
        Confirmed::Recorded(session_id) => Ok(Json(json!({
            "tenko_session_id": session_id,
            "tenko_method": method,
        }))
        .into_response()),
        Confirmed::Conflict {
            error,
            tenko_method,
        } => Ok((
            StatusCode::CONFLICT,
            Json(json!({ "error": error, "tenko_method": tenko_method })),
        )
            .into_response()),
    }
}

/// 見回り 1 回の件数 (ログに出すのは件数だけ。id・テナントは出さない)
#[derive(Debug, Default, PartialEq)]
pub struct SweepSummary {
    /// 一覧の件数
    pub listed: usize,
    /// `通常点呼` で確定した (または既に通常点呼で確定済みだった)
    pub confirmed: usize,
    /// 確定しなかった (先に別の方法で確定された・点呼にならない結果など)
    pub skipped: usize,
    /// DB の失敗 (その測定の transaction は戻り、入力は残る = 次の回にまた拾う)
    pub failed: usize,
}

/// 確定待ちのまま 2 分を過ぎた測定を、1 件ずつ (その測定のテナントと dev の軸で) `通常点呼` で確定する。
/// 1 件の失敗で残りを止めない。一覧が取れなければ `None`
pub async fn sweep(pg: &mut PgClient, log: &LogSink) -> Option<SweepSummary> {
    // 一覧は関数 (SECURITY DEFINER) がテナントをまたいで引く。kit にテナント無しの transaction の口は無いので nil で開く
    let listed = pg
        .tenant_tx(Uuid::nil(), move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(sql::LIST_PENDING, &[]).await?;
                Ok(rows
                    .iter()
                    .map(|r| {
                        (
                            r.get::<_, Uuid>(0),
                            r.get::<_, Uuid>(1),
                            r.get::<_, bool>(2),
                        )
                    })
                    .collect::<Vec<_>>())
            })
        })
        .await;
    let pending = match listed {
        Ok(p) => p,
        Err(e) => {
            log(&format!(
                "measurements: tenko sweep list failed: db ({})",
                kind(&e)
            ));
            return None;
        }
    };
    let mut summary = SweepSummary {
        listed: pending.len(),
        ..Default::default()
    };
    for (tenant_id, id, dev) in pending {
        let method = SWEEP_METHOD.to_owned();
        match confirm(pg, tenant_id, dev, id, method, log.clone()).await {
            Ok(Confirmed::Recorded(_)) => summary.confirmed += 1,
            Ok(_) => summary.skipped += 1,
            Err(e) => {
                log(&format!(
                    "measurements: tenko sweep confirm failed: db ({})",
                    kind(&e)
                ));
                summary.failed += 1;
            }
        }
    }
    Some(summary)
}

impl SweepSummary {
    /// ログの 1 行 (件数だけ)
    pub fn line(&self) -> String {
        format!(
            "measurements: tenko sweep: listed {} confirmed {} skipped {} failed {}",
            self.listed, self.confirmed, self.skipped, self.failed
        )
    }
}
