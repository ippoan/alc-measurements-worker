//! `/api/measurements` の 5 口 (rust-alc-api の `alc-misc` の measurements.rs の `router` と repo/measurements.rs の写し)。
//!
//! - `POST /measurements/start` — `employee_id` だけで status = `started` の行を作る (201)
//! - `PUT /measurements/{id}` — 来た欄だけ COALESCE で変える (検証の 400・無ければ 404)。印 `record_as_tenko` 付きで
//!   完了すれば通常点呼の記録も作り ([`crate::tenko`])、応答に `tenko_session_id` を足す。`defer_tenko` も付けば記録は作らず
//!   確定待ち (`tenko_pending`) にする (rust に無い。[`crate::deferred`])
//! - `POST /measurements` — 完了済みの測定を 1 件作る (201。オフラインの送り直しと同じ形)。点呼の記録も PUT と同じ
//!   (ただし点呼方法・本人確認の方法は受けない = 通常点呼・記録なし)
//! - `GET /measurements` — 絞り込み (乗務員・結果・期間・status) と `measured_at` の新しい順のページ
//!   (`per_page` 既定 50・上限 100、`page` 既定 1・下限 1)
//! - `GET /measurements/{id}` — 1 件 (無ければ 404)
//!
//! 顔写真・動画の口 (`/measurements/{id}/face-photo`・`/video`) は [`crate::files`]。
//! アルコール検知メールのテスト送信 (`POST /measurements/alert-test`。admin だけ) は [`crate::alert::alert_test`]。
//! 血圧の日報のテスト送信 (`POST /measurements/bp-report-test`。admin だけ) は [`crate::report::bp_report_test`]。
//! 点呼の確定 (`POST /measurements/{id}/tenko`。rust に無い) は [`crate::deferred::confirm_tenko`]。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::PgClient;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Error;
use uuid::Uuid;

use crate::alert::{self, should_alert, PendingAlert};
use crate::bp::{self, should_notify, PendingBp};
use crate::carins::normalize_carins_numbers;
use crate::models::{
    CreateMeasurement, Measurement, MeasurementFilter, MeasurementsResponse, StartMeasurement,
    UpdateMeasurement, UpdatedMeasurement, MEASUREMENT_COLUMNS, MEASUREMENT_COLUMN_COUNT,
};
use crate::report;
use crate::tenko::alcohol::{advance, lock_for_measurement};
use crate::tenko::fire::{Event, PendingFire};
use crate::tenko::{record_as_tenko_if_marked, NormalTenkoInput, TenkoSession};
use crate::{set_device_dev, AppState, LogSink};

/// 測定の結果 (`measurements_result_check` と同じ 5 つ)
pub const VALID_RESULTS: [&str; 5] = ["pass", "fail", "normal", "over", "error"];

/// 測定の状態 (`measurements_status_check` と同じ 2 つ)
pub const VALID_STATUSES: [&str; 2] = ["started", "completed"];

/// 通常点呼の記録に付けられる種別 (`tenko_sessions_tenko_type_check` と同じ 3 つ)
pub const VALID_TENKO_TYPES: [&str; 3] = ["normal", "pre_operation", "post_operation"];

/// 測定の保存 (PUT) で指定できる点呼方法。`自動点呼` / `遠隔点呼` は点呼の段を踏む
/// 別の流れなので、この口からは作らせない (Refs ippoan/alc-app#387)
pub const VALID_TENKO_METHODS: [&str; 2] = ["通常点呼", "IT点呼"];

/// 測定の保存 (PUT) で指定できる、運転者の本人確認の方法
/// (`tenko_sessions_identity_method_check` と同じ 5 つ。Refs ippoan/alc-app#387)
pub const VALID_IDENTITY_METHODS: [&str; 5] =
    ["license", "ic_card", "remote_punch", "nfc_card", "manual"];

/// 一覧の `per_page` の既定と上限、`page` の既定 (= 下限)
pub const DEFAULT_PER_PAGE: i64 = 50;
pub const MAX_PER_PAGE: i64 = 100;
pub const DEFAULT_PAGE: i64 = 1;

pub mod sql {
    use crate::models::MEASUREMENT_COLUMNS;

    pub fn start() -> String {
        format!(
            "INSERT INTO measurements (tenant_id, employee_id, status, device_id) \
             VALUES ($1, $2, 'started', $3) \
             RETURNING {MEASUREMENT_COLUMNS}"
        )
    }

    pub fn create() -> String {
        format!(
            "INSERT INTO measurements ( \
                 tenant_id, employee_id, alcohol_level, result, \
                 face_photo_url, measured_at, device_use_count, \
                 temperature, systolic, diastolic, pulse, medical_measured_at, \
                 face_verified, medical_manual_input, video_url, status, device_id \
             ) \
             VALUES ($1, $2, $3, $4, $5, COALESCE($6, NOW()), COALESCE($7, 0), \
                     $8, $9, $10, $11, $12, $13, $14, $15, 'completed', $16) \
             RETURNING {MEASUREMENT_COLUMNS}"
        )
    }

    /// 来た欄だけ変える。`$17` (確定を後回しにする点呼の入力) は、この保存で status が completed になる回
    /// (SET の右辺の `status` は保存の前の値) にだけ `tenko_pending` へ入れる — 後の保存 (録画の URL 等) では上書きしない。
    /// 最後の列 (測定の列の後ろ) は保存の後に確定待ちかどうか
    pub fn update() -> String {
        format!(
            "UPDATE measurements SET \
                 status = COALESCE($1, status), \
                 alcohol_level = COALESCE($2, alcohol_level), \
                 result = COALESCE($3, result), \
                 face_photo_url = COALESCE($4, face_photo_url), \
                 measured_at = COALESCE($5, measured_at), \
                 device_use_count = COALESCE($6, device_use_count), \
                 temperature = COALESCE($7, temperature), \
                 systolic = COALESCE($8, systolic), \
                 diastolic = COALESCE($9, diastolic), \
                 pulse = COALESCE($10, pulse), \
                 medical_measured_at = COALESCE($11, medical_measured_at), \
                 face_verified = COALESCE($12, face_verified), \
                 medical_manual_input = COALESCE($13, medical_manual_input), \
                 video_url = COALESCE($14, video_url), \
                 tenko_pending = CASE WHEN $17::jsonb IS NOT NULL AND status <> 'completed' \
                                       AND COALESCE($1, status) = 'completed' \
                                      THEN $17 ELSE tenko_pending END, \
                 updated_at = NOW() \
             WHERE id = $15 AND tenant_id = $16 \
             RETURNING {MEASUREMENT_COLUMNS}, tenko_pending IS NOT NULL"
        )
    }

    /// 保存の前の値 (`FOR UPDATE`。同じ測定への PUT が並んでも、アルコール検知メール・血圧の通知の条件を満たす保存は 1 つだけ)。
    /// DB の列名は `alcohol_level` (API の欄 `alcohol_value` ではない)。列は (アルコール値, 最高血圧, 乗務員)
    pub const OLD_VALUES: &str = "SELECT alcohol_level, systolic, employee_id FROM measurements \
         WHERE id = $1 AND tenant_id = $2 FOR UPDATE";

    pub fn get() -> String {
        format!("SELECT {MEASUREMENT_COLUMNS} FROM measurements WHERE id = $1 AND tenant_id = $2")
    }
}

type Param<'a> = (&'a (dyn ToSql + Sync), Type);

/// 一覧の WHERE と、その順の値 (rust の動的 WHERE と同じ順: tenant → 乗務員 → 結果 → 期間の始め → 終わり → status)。
/// 値の位置 (`$n`) は値を積んだ順で決まる (条件と値を 1 か所で積むので、ずれない)
pub fn list_where<'a>(tenant_id: &'a Uuid, f: &'a MeasurementFilter) -> (String, Vec<Param<'a>>) {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<Param<'a>> = Vec::new();
    let mut push = |condition: &str, value: &'a (dyn ToSql + Sync), ty: Type| {
        params.push((value, ty));
        conditions.push(format!("{condition} ${}", params.len()));
    };
    push("m.tenant_id =", tenant_id, Type::UUID);
    if let Some(v) = &f.employee_id {
        push("m.employee_id =", v, Type::UUID);
    }
    if let Some(v) = &f.result_type {
        push("m.result =", v, Type::TEXT);
    }
    if let Some(v) = &f.date_from {
        push("m.measured_at >=", v, Type::TIMESTAMPTZ);
    }
    if let Some(v) = &f.date_to {
        push("m.measured_at <=", v, Type::TIMESTAMPTZ);
    }
    if let Some(v) = &f.status {
        push("m.status =", v, Type::TEXT);
    }
    (conditions.join(" AND "), params)
}

/// 一覧の (件数の文, 本体の文)。本体の LIMIT / OFFSET は WHERE の値の後ろの 2 つ
pub fn list_sql(where_clause: &str, n_params: usize) -> (String, String) {
    let cols = MEASUREMENT_COLUMNS
        .split(", ")
        .map(|c| format!("m.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    (
        format!("SELECT COUNT(*) FROM measurements m WHERE {where_clause}"),
        format!(
            "SELECT {cols} FROM measurements m WHERE {where_clause} \
             ORDER BY m.measured_at DESC LIMIT ${} OFFSET ${}",
            n_params + 1,
            n_params + 2
        ),
    )
}

pub async fn start(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    input: StartMeasurement,
    device_id: Option<String>,
) -> Result<Measurement, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_one(
                    &sql::start(),
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.employee_id, Type::UUID),
                        (&device_id, Type::TEXT),
                    ],
                )
                .await?;
            Ok(Measurement::from_row(&row))
        })
    })
    .await
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    input: CreateMeasurement,
    device_id: Option<String>,
    log: LogSink,
) -> Result<(Measurement, bool), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_one(
                    &sql::create(),
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.employee_id, Type::UUID),
                        (&input.alcohol_value, Type::FLOAT8),
                        (&input.result_type, Type::TEXT),
                        (&input.face_photo_url, Type::TEXT),
                        (&input.measured_at, Type::TIMESTAMPTZ),
                        (&input.device_use_count, Type::INT4),
                        (&input.temperature, Type::FLOAT8),
                        (&input.systolic, Type::INT4),
                        (&input.diastolic, Type::INT4),
                        (&input.pulse, Type::INT4),
                        (&input.medical_measured_at, Type::TIMESTAMPTZ),
                        (&input.face_verified, Type::BOOL),
                        (&input.medical_manual_input, Type::BOOL),
                        (&input.video_url, Type::TEXT),
                        (&device_id, Type::TEXT),
                    ],
                )
                .await?;
            let m = Measurement::from_row(&row);
            // INSERT なので旧値は無い (新が 0 より上なら検知メールを送る)
            let alert = should_alert(None, m.alcohol_level);
            // POST は点呼方法・本人確認の方法を受けない (従来どおり通常点呼・記録なし)。点呼セッションの id も返さない
            let tenko = NormalTenkoInput {
                tenko_type: input.tenko_type,
                tenko_method: None,
                identity_method: None,
                carins_cert_no: input.carins_cert_no,
                carins_vehicle_id: input.carins_vehicle_id,
            };
            record_as_tenko_if_marked(tx, &m, input.record_as_tenko, &tenko, &log).await;
            Ok((m, alert))
        })
    })
    .await
}

/// 保存が起こす通知の条件 (どちらも更新後の行と旧値で決まる。送るかどうかはこのあと宛先を引いて決める)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Triggers {
    /// アルコール検知メール ([`should_alert`])
    pub alert: bool,
    /// 血圧の本人への LINE WORKS ([`should_notify`])
    pub bp: bool,
}

impl alc_worker_db::TxOutput for Triggers {}

/// 測定の保存の結果
pub enum Updated {
    /// 測定が無い (404)
    NotFound,
    /// `tenko_session_id` のセッションが無い・乗務員が違う・進められない status (409。何も書いていない)
    SessionMismatch,
    /// 保存した。点呼の webhook (アルコール検知) があれば応答の後に渡す
    Saved(Box<UpdatedMeasurement>, Triggers, Option<Event>),
}

/// transaction の中の結果 (測定・(点呼セッションの id, 確定待ち)・通知の条件・(進めたセッション, webhook))
type Saved = (
    Measurement,
    (Option<Uuid>, bool),
    Triggers,
    Option<(TenkoSession, Option<Event>)>,
);

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    input: UpdateMeasurement,
    log: LogSink,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Updated, Error> {
    // 外側の None = セッションが合わない、内側の None = 測定が無い
    let found: Option<Option<Saved>> = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                set_device_dev(tx, dev).await?;
                let tenko = NormalTenkoInput {
                    tenko_type: input.tenko_type,
                    tenko_method: input.tenko_method,
                    identity_method: input.identity_method,
                    carins_cert_no: input.carins_cert_no,
                    carins_vehicle_id: input.carins_vehicle_id,
                };
                // 確定を後回しにするなら、記録の入力 (点呼方法を除く) を列に入れる (入れるかどうかの判定は UPDATE の中)
                let pending =
                    (input.record_as_tenko && input.defer_tenko == Some(true)).then(|| {
                        serde_json::to_value(&tenko).expect("NormalTenkoInput は JSON にできる")
                    });
                // 旧値は UPDATE の前に読む (点呼の記録の区切りより前)。行が無ければ UPDATE も何も返さず Ok(None)
                let old = tx
                    .query_typed_opt(
                        sql::OLD_VALUES,
                        &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                    )
                    .await?;
                let (old_level, old_systolic) = old.as_ref().map_or((None, None), |r| {
                    (r.get::<_, Option<f64>>(0), r.get::<_, Option<i32>>(1))
                });
                // アルコールの段を進めるセッションは測定の UPDATE の前に押さえる (合わなければ何も書かずに 409)。
                // 測定が無ければ下の UPDATE が 0 行で 404
                let session = match (input.tenko_session_id, &old) {
                    (Some(sid), Some(r)) => {
                        match lock_for_measurement(tx, tenant_id, sid, id, r.get(2)).await? {
                            Some(cur) => Some(cur),
                            None => return Ok(None),
                        }
                    }
                    _ => None,
                };
                let row = tx
                    .query_typed_opt(
                        &sql::update(),
                        &[
                            (&input.status, Type::TEXT),
                            (&input.alcohol_value, Type::FLOAT8),
                            (&input.result_type, Type::TEXT),
                            (&input.face_photo_url, Type::TEXT),
                            (&input.measured_at, Type::TIMESTAMPTZ),
                            (&input.device_use_count, Type::INT4),
                            (&input.temperature, Type::FLOAT8),
                            (&input.systolic, Type::INT4),
                            (&input.diastolic, Type::INT4),
                            (&input.pulse, Type::INT4),
                            (&input.medical_measured_at, Type::TIMESTAMPTZ),
                            (&input.face_verified, Type::BOOL),
                            (&input.medical_manual_input, Type::BOOL),
                            (&input.video_url, Type::TEXT),
                            (&id, Type::UUID),
                            (&tenant_id, Type::UUID),
                            (&pending, Type::JSONB),
                        ],
                    )
                    .await?;
                let Some(row) = row else {
                    return Ok(Some(None));
                };
                let m = Measurement::from_row(&row);
                let tenko_pending: bool = row.get(MEASUREMENT_COLUMN_COUNT);
                // 入力に値が無くても COALESCE で旧値が残るので、input ではなく UPDATE 後の m で見る
                let triggers = Triggers {
                    alert: should_alert(old_level, m.alcohol_level),
                    bp: should_notify(old_systolic, &m),
                };
                // 確定待ちの測定 (この保存で入れた、または前の保存で入れてまだ確定していない) は記録を作らない
                // (確定の口か見回りが作る)
                let marked = input.record_as_tenko && !tenko_pending;
                let session_id = record_as_tenko_if_marked(tx, &m, marked, &tenko, &log).await;
                let advanced = match session {
                    Some(cur) => Some(advance(tx, cur, &m, now, &log).await),
                    None => None,
                };
                Ok(Some(Some((
                    m,
                    (session_id, tenko_pending),
                    triggers,
                    advanced,
                ))))
            })
        })
        .await?;
    let Some(found) = found else {
        return Ok(Updated::SessionMismatch);
    };
    let Some((measurement, (session_id, tenko_pending), triggers, advanced)) = found else {
        return Ok(Updated::NotFound);
    };
    let (tenko_session, event) = match advanced {
        Some((s, event)) => (Some(s), event),
        None => (None, None),
    };
    Ok(Updated::Saved(
        Box::new(UpdatedMeasurement {
            measurement,
            tenko_session_id: tenko_session.as_ref().map(|s| s.id).or(session_id),
            tenko_pending,
            tenko_session,
        }),
        triggers,
        event,
    ))
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
) -> Result<Option<Measurement>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_opt(&sql::get(), &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(Measurement::from_row))
        })
    })
    .await
}

/// (その頁の行, 絞り込みに合う件数)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: MeasurementFilter,
    page: i64,
    per_page: i64,
) -> Result<(Vec<Measurement>, i64), Error> {
    let offset = (page - 1) * per_page;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let (where_clause, mut params) = list_where(&tenant_id, &filter);
            let (count_sql, rows_sql) = list_sql(&where_clause, params.len());
            let total: i64 = tx.query_typed_one(&count_sql, &params).await?.get(0);
            params.push((&per_page, Type::INT8));
            params.push((&offset, Type::INT8));
            let rows = tx.query_typed(&rows_sql, &params).await?;
            Ok((rows.iter().map(Measurement::from_row).collect(), total))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/measurements",
            post(create_measurement).get(list_measurements),
        )
        .route("/measurements/start", post(start_measurement))
        .route("/measurements/alert-test", post(alert::alert_test))
        .route("/measurements/bp-report-test", post(report::bp_report_test))
        .route(
            "/measurements/{id}",
            get(get_measurement).put(update_measurement),
        )
        .route(
            "/measurements/{id}/tenko",
            post(crate::deferred::confirm_tenko),
        )
}

/// 端末の ID を載せるヘッダー。auth-worker が端末の token で来た要求にだけ付ける (検証済み token の sub。client の同名ヘッダーは届かない)
const DEVICE_ID_HEADER: &str = "x-device-id";
const MAX_DEVICE_ID_LEN: usize = 64;

/// `X-Device-Id` を読む (本文からは読まない)。無い・1〜64 字の `[A-Za-z0-9:_-]` でない (alc-vein-worker の
/// `registered_device_id` と同じ規則) なら `None` (= device_id は NULL)。**形が外れても測定は作る**
/// (端末の ID は録画の切り出し用の付帯情報で、それで測定を失わない)。外れたときは値を出さずログ 1 行だけ
pub fn device_id_from(headers: &HeaderMap, log: &LogSink) -> Option<String> {
    let value = headers.get(DEVICE_ID_HEADER)?;
    let valid = value.to_str().ok().filter(|s| {
        (1..=MAX_DEVICE_ID_LEN).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-'))
    });
    if valid.is_none() {
        log("measurements: X-Device-Id ignored: invalid shape");
    }
    valid.map(str::to_owned)
}

fn check(valid: bool) -> Result<(), StatusCode> {
    valid.then_some(()).ok_or(StatusCode::BAD_REQUEST)
}

/// 来ていれば `allowed` のどれかであること (来ていなければ通す)
fn check_opt(value: &Option<String>, allowed: &[&str]) -> Result<(), StatusCode> {
    check(value.as_deref().is_none_or(|v| allowed.contains(&v)))
}

/// 測定開始 — employee_id のみで status='started' レコードを作成
async fn start_measurement(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    headers: HeaderMap,
    Json(body): Json<StartMeasurement>,
) -> Result<(StatusCode, Json<Measurement>), StatusCode> {
    let device_id = device_id_from(&headers, &state.log);
    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let m = start(&mut pg, tenant_id, dev, body, device_id)
        .await
        .map_err(|e| state.db_fail("measurement start", &e))?;
    Ok((StatusCode::CREATED, Json(m)))
}

/// 測定更新 — COALESCE で各フィールドを段階的に更新
async fn update_measurement(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(mut body): Json<UpdateMeasurement>,
) -> Result<
    (
        Option<Extension<PendingAlert>>,
        Option<Extension<PendingBp>>,
        Option<Extension<PendingFire>>,
        Json<UpdatedMeasurement>,
    ),
    StatusCode,
> {
    check_opt(&body.result_type, &VALID_RESULTS)?;
    check_opt(&body.status, &VALID_STATUSES)?;
    check_opt(&body.tenko_type, &VALID_TENKO_TYPES)?;
    check_opt(&body.tenko_method, &VALID_TENKO_METHODS)?;
    check_opt(&body.identity_method, &VALID_IDENTITY_METHODS)?;
    normalize_carins_numbers(&mut body.carins_cert_no, &mut body.carins_vehicle_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    // 通常点呼の新しいセッションと、既存のセッションを進めるのは同時にできない
    check(!(body.record_as_tenko && body.tenko_session_id.is_some()))?;

    let dev = dev.get();
    let now = (state.now)();
    let mut pg = state.pg.lock().await;
    let found = update(&mut pg, tenant_id, dev, id, body, state.log.clone(), now)
        .await
        .map_err(|e| state.db_fail("measurement update", &e))?;
    let (updated, triggers, event) = match found {
        Updated::Saved(u, t, e) => (*u, t, e),
        Updated::NotFound => return Err(StatusCode::NOT_FOUND),
        Updated::SessionMismatch => return Err(StatusCode::CONFLICT),
    };
    let fire = PendingFire::new(&state, tenant_id, dev, event.into_iter().collect());
    // 保存は commit 済み。宛先は lock を持ったまま引き、写真の読みと送信は応答の後 (Worker の wait_until)
    let m = &updated.measurement;
    let outbox = alert::prepare(&state, &mut pg, tenant_id, dev, triggers.alert, m).await;
    let pending = PendingAlert::new(&state, tenant_id, outbox, m.face_photo_url.as_deref());
    let bp = bp::prepare(&state, &mut pg, tenant_id, dev, triggers.bp, m).await;
    Ok((
        pending.map(Extension),
        bp.map(Extension),
        fire.map(Extension),
        Json(updated),
    ))
}

/// 測定作成 (完了済み) — オフラインキュー互換
async fn create_measurement(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    headers: HeaderMap,
    Json(mut body): Json<CreateMeasurement>,
) -> Result<
    (
        StatusCode,
        Option<Extension<PendingAlert>>,
        Json<Measurement>,
    ),
    StatusCode,
> {
    let device_id = device_id_from(&headers, &state.log);
    check(VALID_RESULTS.contains(&body.result_type.as_str()))?;
    check_opt(&body.tenko_type, &VALID_TENKO_TYPES)?;
    normalize_carins_numbers(&mut body.carins_cert_no, &mut body.carins_vehicle_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let (m, alert) = create(&mut pg, tenant_id, dev, body, device_id, state.log.clone())
        .await
        .map_err(|e| state.db_fail("measurement create", &e))?;
    let outbox = alert::prepare(&state, &mut pg, tenant_id, dev, alert, &m).await;
    let pending = PendingAlert::new(&state, tenant_id, outbox, m.face_photo_url.as_deref());
    Ok((StatusCode::CREATED, pending.map(Extension), Json(m)))
}

async fn list_measurements(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<MeasurementFilter>,
) -> Result<Json<MeasurementsResponse>, StatusCode> {
    let per_page = filter
        .per_page
        .unwrap_or(DEFAULT_PER_PAGE)
        .min(MAX_PER_PAGE);
    let page = filter.page.unwrap_or(DEFAULT_PAGE).max(DEFAULT_PAGE);

    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let (measurements, total) = list(&mut pg, tenant_id, dev, filter, page, per_page)
        .await
        .map_err(|e| state.db_fail("measurement list", &e))?;
    Ok(Json(MeasurementsResponse {
        measurements,
        total,
        page,
        per_page,
    }))
}

async fn get_measurement(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Json<Measurement>, StatusCode> {
    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, dev, id)
        .await
        .map_err(|e| state.db_fail("measurement get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn list_where_numbers_params_in_rust_order() {
        let tenant = Uuid::nil();
        let at: DateTime<Utc> = "2026-09-12T00:00:00Z".parse().unwrap();
        let none = MeasurementFilter::default();
        let (w, p) = list_where(&tenant, &none);
        assert_eq!(w, "m.tenant_id = $1");
        assert_eq!(p.len(), 1);

        let all = MeasurementFilter {
            employee_id: Some(Uuid::nil()),
            result_type: Some("pass".into()),
            date_from: Some(at),
            date_to: Some(at),
            status: Some("completed".into()),
            ..Default::default()
        };
        let (w, p) = list_where(&tenant, &all);
        assert_eq!(
            w,
            "m.tenant_id = $1 AND m.employee_id = $2 AND m.result = $3 AND m.measured_at >= $4 \
             AND m.measured_at <= $5 AND m.status = $6"
        );
        let types: Vec<Type> = p.iter().map(|(_, t)| t.clone()).collect();
        assert_eq!(
            types,
            [
                Type::UUID,
                Type::UUID,
                Type::TEXT,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMPTZ,
                Type::TEXT
            ]
        );

        // 間の条件が抜けても番号は詰まる
        let some = MeasurementFilter {
            status: Some("started".into()),
            ..Default::default()
        };
        assert_eq!(
            list_where(&tenant, &some).0,
            "m.tenant_id = $1 AND m.status = $2"
        );
    }

    #[test]
    fn list_sql_puts_limit_offset_after_the_filter() {
        let (count, rows) = list_sql("m.tenant_id = $1 AND m.status = $2", 2);
        assert_eq!(
            count,
            "SELECT COUNT(*) FROM measurements m WHERE m.tenant_id = $1 AND m.status = $2"
        );
        assert!(rows.starts_with("SELECT m.id, m.tenant_id, m.employee_id, m.alcohol_level, "));
        assert!(rows.ends_with(
            "WHERE m.tenant_id = $1 AND m.status = $2 ORDER BY m.measured_at DESC LIMIT $3 OFFSET $4"
        ));
    }

    #[test]
    fn measurement_column_count_matches_the_columns() {
        // UPDATE の RETURNING は測定の列の後ろに確定待ちの印を足し、その位置で読む
        assert_eq!(
            MEASUREMENT_COLUMNS.split(", ").count(),
            MEASUREMENT_COLUMN_COUNT
        );
    }

    #[test]
    fn check_opt_passes_missing_and_listed_values() {
        assert_eq!(check_opt(&None, &VALID_STATUSES), Ok(()));
        assert_eq!(check_opt(&Some("started".into()), &VALID_STATUSES), Ok(()));
        assert_eq!(
            check_opt(&Some("done".into()), &VALID_STATUSES),
            Err(StatusCode::BAD_REQUEST)
        );
    }
}
