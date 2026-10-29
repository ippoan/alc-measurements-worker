//! 測定の口の入出力の形 (rust-alc-api の `alc_core::models` の Measurement 系の写し)。
//! 欄の名前・順・serde の rename (`alcohol_level` → `alcohol_value`、`result` → `result_type`) と
//! alias (`alcohol_level`・`result`・`from` → `date_from`・`to` → `date_to`) は rust と同じ。

use alc_worker_db::TxOutput;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;
use uuid::Uuid;

/// 測定の行 (rust の `Measurement`。DB の行の欄を全部持ち、JSON の key の順もこの並び)。
/// 列の読み順は [`MEASUREMENT_COLUMNS`] と同じ
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Measurement {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub employee_id: Uuid,
    #[serde(rename = "alcohol_value")]
    pub alcohol_level: Option<f64>,
    #[serde(rename = "result_type")]
    pub result: Option<String>,
    pub device_use_count: i32,
    pub face_photo_url: Option<String>,
    pub video_url: Option<String>,
    pub measured_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: String,
    // Medical data (BLE Medical Gateway)
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub face_verified: Option<bool>,
    pub medical_manual_input: Option<bool>,
    /// この行を作った端末の ID (auth-worker が端末の token から付けた `X-Device-Id`。無ければ NULL。alc-migrations 190)
    pub device_id: Option<String>,
}

impl TxOutput for Measurement {}

/// rust の `SELECT *` / `RETURNING *` の代わり。[`Measurement`] の欄の順 (Worker は位置で読む)。
/// 表には `is_dev` も在るが、rust の行型に無いので読まない (応答にも出ない)
pub const MEASUREMENT_COLUMNS: &str = "id, tenant_id, employee_id, alcohol_level, result, device_use_count, \
     face_photo_url, video_url, measured_at, created_at, updated_at, status, \
     temperature, systolic, diastolic, pulse, medical_measured_at, face_verified, medical_manual_input, \
     device_id";

/// [`MEASUREMENT_COLUMNS`] の列の数 (= その後ろに足した列の位置)
pub const MEASUREMENT_COLUMN_COUNT: usize = 20;

impl Measurement {
    /// [`MEASUREMENT_COLUMNS`] の順で並んだ行から読む
    pub fn from_row(r: &Row) -> Self {
        Self {
            id: r.get(0),
            tenant_id: r.get(1),
            employee_id: r.get(2),
            alcohol_level: r.get(3),
            result: r.get(4),
            device_use_count: r.get(5),
            face_photo_url: r.get(6),
            video_url: r.get(7),
            measured_at: r.get(8),
            created_at: r.get(9),
            updated_at: r.get(10),
            status: r.get(11),
            temperature: r.get(12),
            systolic: r.get(13),
            diastolic: r.get(14),
            pulse: r.get(15),
            medical_measured_at: r.get(16),
            face_verified: r.get(17),
            medical_manual_input: r.get(18),
            device_id: r.get(19),
        }
    }
}

/// `POST /api/measurements` の本文 (rust の `CreateMeasurement`)
#[derive(Debug, Deserialize)]
pub struct CreateMeasurement {
    pub employee_id: Uuid,
    /// 通常点呼として点呼セッション・点呼記録も作るかどうか (運行者端末からの保存で true)。
    /// 既存の呼び元は送ってこないので default = false。
    #[serde(default)]
    pub record_as_tenko: bool,
    /// 通常点呼の記録の種別 (`normal` / `pre_operation` / `post_operation`)。
    /// 運行者端末で始業/終業を選んだときだけ送られる。無ければ `normal`。
    #[serde(default)]
    pub tenko_type: Option<String>,
    /// 運行者端末が電子車検証から読んだ管理番号 (数字 12〜13 桁)。通常点呼の記録に写す
    /// (Refs ippoan/alc-app-s3#110)。空文字は無しと同じ
    #[serde(default)]
    pub carins_cert_no: Option<String>,
    /// 同じく車両 ID (英数 14 桁)
    #[serde(default)]
    pub carins_vehicle_id: Option<String>,
    #[serde(alias = "alcohol_level")]
    pub alcohol_value: f64,
    #[serde(alias = "result")]
    pub result_type: String,
    pub face_photo_url: Option<String>,
    pub video_url: Option<String>,
    pub measured_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub device_use_count: Option<i32>,
    // Medical data (BLE Medical Gateway)
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub face_verified: Option<bool>,
    pub medical_manual_input: Option<bool>,
}

/// `POST /api/measurements/start` の本文 (rust の `StartMeasurement`)
#[derive(Debug, Deserialize)]
pub struct StartMeasurement {
    pub employee_id: Uuid,
}

/// `PUT /api/measurements/{id}` の本文 (rust の `UpdateMeasurement`)
#[derive(Debug, Deserialize)]
pub struct UpdateMeasurement {
    pub status: Option<String>,
    /// status が completed になる保存でのみ効く。詳細は [`CreateMeasurement::record_as_tenko`]。
    #[serde(default)]
    pub record_as_tenko: bool,
    /// 詳細は [`CreateMeasurement::tenko_type`]。
    #[serde(default)]
    pub tenko_type: Option<String>,
    /// 点呼セッション・点呼記録に書く点呼方法 (`通常点呼` / `IT点呼`)。無ければ `通常点呼`。
    /// 種別 (`tenko_type`) とは軸が違う。POST ([`CreateMeasurement`]) には無い
    /// (Refs ippoan/alc-app#387)
    #[serde(default)]
    pub tenko_method: Option<String>,
    /// 運転者の本人確認の方法 (`license` / `ic_card` / `remote_punch` / `nfc_card` / `manual`)。
    /// 点呼セッションに書くだけで、認可の判断には使わない。無ければ NULL (記録なし)。
    /// POST ([`CreateMeasurement`]) には無い (Refs ippoan/alc-app#387)
    #[serde(default)]
    pub identity_method: Option<String>,
    /// 詳細は [`CreateMeasurement::carins_cert_no`]。
    #[serde(default)]
    pub carins_cert_no: Option<String>,
    /// 詳細は [`CreateMeasurement::carins_vehicle_id`]。
    #[serde(default)]
    pub carins_vehicle_id: Option<String>,
    /// `record_as_tenko` と一緒に `true` なら、この保存では点呼の記録を作らず、記録の入力 (点呼方法を除く) を
    /// 測定の列 `tenko_pending` に入れて確定を待つ (status が completed になる保存の回だけ。後の保存では上書きしない)。
    /// 確定は `POST /measurements/{id}/tenko` か、見回り (2 分後に `通常点呼`)。rust には無い (Refs ippoan/alc-app#436)
    #[serde(default)]
    pub defer_tenko: Option<bool>,
    /// 自動点呼・遠隔点呼の本人確認済みのセッション。status が completed の保存で、**この測定の行の**結果からアルコールの段を
    /// 進める ([`crate::tenko::alcohol`])。`record_as_tenko` と一緒には付けられない (400)。セッションが無い・乗務員が違う・
    /// 進められない status なら保存ごと 409 (Refs ippoan/rust-alc-api#680)
    #[serde(default)]
    pub tenko_session_id: Option<Uuid>,
    #[serde(alias = "alcohol_level")]
    pub alcohol_value: Option<f64>,
    #[serde(alias = "result")]
    pub result_type: Option<String>,
    pub face_photo_url: Option<String>,
    pub video_url: Option<String>,
    pub measured_at: Option<DateTime<Utc>>,
    pub device_use_count: Option<i32>,
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub face_verified: Option<bool>,
    pub medical_manual_input: Option<bool>,
}

/// `PUT /api/measurements/{id}` の応答 (rust の `UpdatedMeasurement`)。[`Measurement`] の欄がそのまま並び、
/// `tenko_session_id` が 1 つ増えるだけ (Refs ippoan/alc-app#387)。
#[derive(Debug, Serialize)]
pub struct UpdatedMeasurement {
    #[serde(flatten)]
    pub measurement: Measurement,
    /// この保存で作られた (または同じ測定に既にある) 通常の流れの点呼セッションの id。
    /// `record_as_tenko` が無い・結果が点呼にならない・記録に失敗した・確定待ちのときは `None`
    pub tenko_session_id: Option<Uuid>,
    /// 点呼の記録が確定待ち (`tenko_pending` が入っている) なら `true`。`false` のときは欄ごと出さない
    /// (確定を後回しにしない保存の応答は今までと同じ形。Refs ippoan/alc-app#436)
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub tenko_pending: bool,
    /// `tenko_session_id` を付けた保存の、進めた後 (進めなかったら今) のセッション。付けない保存の応答には欄ごと出さない
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenko_session: Option<crate::tenko::TenkoSession>,
}

/// `GET /api/measurements` の query (rust の `MeasurementFilter`)
#[derive(Debug, Default, Deserialize)]
pub struct MeasurementFilter {
    pub employee_id: Option<Uuid>,
    #[serde(alias = "result")]
    pub result_type: Option<String>,
    #[serde(alias = "from")]
    pub date_from: Option<DateTime<Utc>>,
    #[serde(alias = "to")]
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub status: Option<String>,
}

/// `GET /api/measurements` の応答 (rust の `MeasurementsResponse`)
#[derive(Debug, Serialize)]
pub struct MeasurementsResponse {
    pub measurements: Vec<Measurement>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}
