//! キオスクの車両番号の候補 `GET /tenko/sessions/{id}/vehicle-candidates?q=<数字>` (自己点検表 業務前 二十 ハ の入力の補助)。
//!
//! テンキーで打った数字で一連指定番号 (登録番号のひらがなの後の数字。全角の数字・区切りの `-`・`・` は無視) が始まる車両を、
//! 車検証 (`car_inspection` の登録番号 `EntryNoCarNo`。車両 ID ごとに最新の 1 枚。`CarNo` は車台番号なので使わない) と
//! 車両マスタ (`maintenance_vehicles`) から最大 [`MAX_CANDIDATES`] 件返す。各候補には点呼の営業所への
//! 紐づけ (手で選んだ所属、無ければ車検証の使用の本拠・使用者の対応。空白は無視して比べ、空白だけ違う対応の行が複数あっても
//! 車両は 1 件で、営業所は office_id の最小の 1 つ (alc-tenko-worker の一覧と同じ規則)。判定 [`super::judgment`] と同じ優先) を付ける。
//! 要求した端末 (`X-Device-Id`) が営業所に所属していれば (migration 207)、その営業所の車両を先に並べる。並びの次は
//! 一連指定番号が打った数字と一致する車両、最後は一連指定番号・登録番号の順。
//!
//! 通すのは車両番号の段 (`PUT …/vehicle`) と同じ (テナントのヘッダーがあればよい)。セッションが無ければ 404、
//! `q` が 1〜8 桁の数字でなければ 400 `{"error": "invalid_query"}`。応答は
//! `{"candidates": [{"vehicle_number", "carins_cert_no", "office_name", "company_name", "linked", "device_office"}]}`。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_postgres::types::Type;
use uuid::Uuid;

use super::flow::{fetch, run, Flow};
use crate::measurements::device_id_from;
use crate::AppState;

/// 返す候補の上限
pub const MAX_CANDIDATES: i64 = 20;

pub mod sql {
    /// 候補。$1 tenant_id / $2 打った数字 / $3 端末の ID (無ければ NULL) / $4 上限
    pub const CANDIDATES: &str = "WITH ci AS ( \
             SELECT DISTINCT ON (c.\"CarId\") btrim(c.\"EntryNoCarNo\") AS no, c.\"ElectCertMgNo\" AS cert, \
                 btrim(c.\"UseheadqrterChar\") AS use_base, \
                 btrim(COALESCE(NULLIF(btrim(c.\"UsernameHighLevelChar\"), ''), c.\"UsernameLowLevelChar\")) AS user_name \
             FROM alc_api.car_inspection c WHERE c.tenant_id = $1 AND btrim(c.\"EntryNoCarNo\") <> '' \
             ORDER BY c.\"CarId\", c.created_at DESC), \
         mv AS ( \
             SELECT v.registration_number AS no, v.office_id FROM alc_api.maintenance_vehicles v \
             WHERE v.tenant_id = $1 AND v.deleted_at IS NULL), \
         cand AS ( \
             SELECT ci.no, ci.cert, COALESCE((SELECT mv.office_id FROM mv WHERE mv.office_id IS NOT NULL \
                 AND regexp_replace(mv.no, '[[:space:]　]', '', 'g') = regexp_replace(ci.no, '[[:space:]　]', '', 'g') \
                 LIMIT 1), (SELECT b.office_id FROM alc_api.tenko_office_carins_bases b \
                 WHERE b.tenant_id = $1 \
                 AND regexp_replace(b.use_base, '[[:space:]　]', '', 'g') = regexp_replace(ci.use_base, '[[:space:]　]', '', 'g') \
                 AND regexp_replace(b.user_name, '[[:space:]　]', '', 'g') = regexp_replace(ci.user_name, '[[:space:]　]', '', 'g') \
                 ORDER BY b.office_id LIMIT 1)) AS office_id \
             FROM ci \
             UNION ALL \
             SELECT mv.no, NULL, mv.office_id FROM mv WHERE NOT EXISTS (SELECT 1 FROM ci \
                 WHERE regexp_replace(ci.no, '[[:space:]　]', '', 'g') = regexp_replace(mv.no, '[[:space:]　]', '', 'g'))), \
         digits AS ( \
             SELECT cand.*, regexp_replace(regexp_replace(translate(cand.no, '０１２３４５６７８９', '0123456789'), \
                 '^.*[^0-9・．.\\-－[:space:]　]', ''), '[^0-9]', '', 'g') AS d \
             FROM cand) \
         SELECT digits.no, digits.cert, o.name, o.company_name, digits.office_id IS NOT NULL, \
             COALESCE(digits.office_id IN (SELECT t.office_id FROM alc_api.tenko_office_devices t \
                 WHERE t.tenant_id = $1 AND t.device_id = $3), FALSE) AS device_office \
         FROM digits LEFT JOIN alc_api.tenko_offices o ON o.tenant_id = $1 AND o.id = digits.office_id \
         WHERE digits.d LIKE $2 || '%' \
         ORDER BY device_office DESC, digits.d = $2 DESC, digits.d, digits.no \
         LIMIT $4";
}

pub fn router() -> Router<AppState> {
    Router::new().route(
        "/tenko/sessions/{id}/vehicle-candidates",
        get(candidates_handler),
    )
}

#[derive(Debug, Deserialize)]
pub struct CandidatesQuery {
    /// テンキーで打った数字
    pub q: String,
}

/// 打った数字が候補の検索に使えるか (1〜8 桁の半角数字)
pub fn valid_query(q: &str) -> bool {
    (1..=8).contains(&q.len()) && q.bytes().all(|b| b.is_ascii_digit())
}

async fn candidates_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<CandidatesQuery>,
) -> Response {
    let device_id = device_id_from(&headers, &state.log);
    let q = query.q;
    run(
        state,
        tenant_id,
        dev.get(),
        "session vehicle candidates",
        false,
        move |tx| {
            Box::pin(async move {
                if fetch(tx, tenant_id, id).await?.is_none() {
                    return Ok(StatusCode::NOT_FOUND.into());
                }
                if !valid_query(&q) {
                    return Ok(Flow::Api((
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "error": "invalid_query" })),
                    )));
                }
                let rows = tx
                    .query_typed(
                        sql::CANDIDATES,
                        &[
                            (&tenant_id, Type::UUID),
                            (&q, Type::TEXT),
                            (&device_id, Type::TEXT),
                            (&MAX_CANDIDATES, Type::INT8),
                        ],
                    )
                    .await?;
                let candidates: Vec<Value> = rows
                    .iter()
                    .map(|r| {
                        json!({
                            "vehicle_number": r.get::<_, String>(0),
                            "carins_cert_no": r.get::<_, Option<String>>(1),
                            "office_name": r.get::<_, Option<String>>(2),
                            "company_name": r.get::<_, Option<String>>(3),
                            "linked": r.get::<_, bool>(4),
                            "device_office": r.get::<_, bool>(5),
                        })
                    })
                    .collect();
                Ok(Flow::Api((
                    StatusCode::OK,
                    Json(json!({ "candidates": candidates })),
                )))
            })
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_must_be_one_to_eight_digits() {
        assert!(valid_query("7601"));
        assert!(valid_query("12345678"));
        assert!(!valid_query(""));
        assert!(!valid_query("123456789"));
        assert!(!valid_query("76a1"));
        assert!(!valid_query("７６"));
    }
}
