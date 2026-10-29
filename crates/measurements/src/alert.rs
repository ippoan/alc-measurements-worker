//! アルコール検知メール (Refs ippoan/alc-app#436)。測定のアルコール値が 0 より上になった保存で、テナントの notify の
//! グループ「アルコール検知」のメンバーへ 1 人 1 通のメールを送る。宛先は notify の既存の表 (`notify_groups`・
//! `notify_recipient_groups`・`notify_recipients`) から引き、専用の表は作らない。
//!
//! # 送る条件
//!
//! アルコール値 (`alcohol_level`。機器の判定 `over` ではない) が **旧が NULL か 0 以下 → 新が 0 より上** になった保存だけ
//! ([`should_alert`])。POST は旧値が無いので新が 0 より上なら送る。同じ測定を >0 のまま保存し直しても、>0 → 0 → >0 と
//! 戻しても、1 回目の >0 の 1 通だけ (戻したあとの >0 は新しい通知)。旧値は PUT の transaction の中で `FOR UPDATE` で読む
//! (同じ測定への PUT が並んでも、条件を満たす保存は 1 つだけ)。
//!
//! # 流れ (lock を握ったまま送らない)
//!
//! 1. 保存の transaction (旧値の読みと判定) が commit される
//! 2. [`prepare`] — pg の lock を持ったまま、短い tenant_tx 1 回で宛先と乗務員名を引く (RLS が効く)。保存とは別の
//!    transaction なので、ここの失敗で保存は巻き戻らない (ログ 1 行で送らない)
//! 3. 応答を返した後 — [`PendingAlert`] を応答の extension に入れ、Worker の fetch が取り出して `wait_until` に渡す。
//!    そこで顔写真を R2 から読んで添付し、[`deliver`] で宛先ごとに 1 通ずつ送る (1 人が未確認の送り先でも他の人へ
//!    届くように)。保存の応答は写真の読みと送信を待たない。失敗はログ 1 行だけで、リトライしない
//!
//! 送るのは [`AlertMailer`] (Worker では Cloudflare の `send_email` binding、テストでは偽物)。無ければ (`None`) 送らない。
//! **ログは固定の語だけ** (メールアドレス・tenant_id・氏名を出さない)。
//!
//! # テスト送信 (`POST /api/measurements/alert-test`)
//!
//! 測定を起こさずに、同じ宛先へ確認用の 1 通ずつを送る ([`alert_test`])。宛先の引き ([`recipients`])・送信のループ
//! ([`deliver`]) は検知と同じものを使う。**この口だけ admin を見る** (`X-User-Role` から復元した `AuthUser`。
//! それ以外は 403)。送り手が無い → 503、宛先が 0 → 409、送った → 200 `{"sent", "failed"}` (宛先ごとの成否・アドレスは返さない)。

use std::sync::Arc;

use alc_core_wasm::{AuthUser, DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TenantTx};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::FixedOffset;
use futures_util::future::BoxFuture;
use serde_json::json;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::files::{read_media, Media, SharedStore};
use crate::models::Measurement;
use crate::tenko::record::sql::EMPLOYEE_NAME;
use crate::{set_device_dev, AppState, LogSink};

/// 宛先のグループの名前 (テナントごとに notify の画面で作る。`notify_groups` は (tenant_id, name) が UNIQUE)
pub const ALERT_GROUP: &str = "アルコール検知";

/// 乗務員の名前が引けないときの表示
pub const UNKNOWN_EMPLOYEE: &str = "不明";

/// 件名の接頭辞 (検知とテスト送信で同じ)
pub const SUBJECT_PREFIX: &str = "アルコール検知: ";

/// 送信の段の失敗。持つのは段の名前だけ (宛先・本文・ランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailError(pub &'static str);

/// 添付 (測定の顔写真・吹き終わりの静止画)。bytes は宛先間で共有する (宛先ごとに複製しない)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub filename: &'static str,
    pub content_type: &'static str,
    pub bytes: Arc<[u8]>,
}

/// 1 人宛の 1 通 (差出人は送り手 [`AlertMailer`] が持つ)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertMail {
    pub to: String,
    pub subject: String,
    pub body: String,
    pub attachment: Option<Attachment>,
}

/// 写真を添付したときだけ本文の末尾に足す 1 行
pub const PHOTO_NOTE: &str = "写真を添付しています。\n";

/// メールの送り手 (Worker では `send_email` の binding `ALERT_EMAIL`、テストでは偽物)。
/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait AlertMailer: Send + Sync {
    fn send<'a>(&'a self, mail: &'a AlertMail) -> BoxFuture<'a, Result<(), MailError>>;
}

pub type SharedMailer = Arc<dyn AlertMailer>;

/// 宛先と乗務員名を引く SQL。グループ → (LEFT JOIN) メンバー → 宛先。グループが在ってメンバーが 0 でも
/// グループの行は 1 行返る (email は NULL)。グループが無ければ 0 行 — 設定漏れと「メンバー 0」を見分けるため。
/// 使えない宛先 (`enabled` でない・email が NULL) は JOIN の条件で落とす (NULL の行になる)。WHERE に tenant_id も明示する
pub const RECIPIENTS: &str = "SELECT r.email \
     FROM notify_groups g \
     LEFT JOIN notify_recipient_groups m ON m.group_id = g.id \
     LEFT JOIN notify_recipients r ON r.id = m.recipient_id \
         AND r.tenant_id = $2 AND r.enabled AND r.email IS NOT NULL \
     WHERE g.name = $1 AND g.tenant_id = $2";

/// 旧が NULL か 0 以下で、新が 0 より上のときだけ true
pub fn should_alert(old: Option<f64>, new: Option<f64>) -> bool {
    new.is_some_and(|v| v > 0.0) && !old.is_some_and(|v| v > 0.0)
}

/// 件名と本文 (日本語。測定日時は日本時間)。乗務員名の改行は空白にする (件名に改行を入れさせない)
pub fn compose(employee_name: &str, m: &Measurement) -> (String, String) {
    let name = employee_name.replace(['\r', '\n'], " ");
    let jst = FixedOffset::east_opt(9 * 3600).expect("+09:00");
    let measured_at = m
        .measured_at
        .with_timezone(&jst)
        .format("%Y-%m-%d %H:%M:%S");
    let level = m.alcohol_level.unwrap_or_default();
    let subject = format!("{SUBJECT_PREFIX}{name}");
    let body = format!(
        "アルコールが検知されました。\n\n\
         乗務員: {name}\n\
         アルコール値: {level}\n\
         測定日時: {measured_at} (日本時間)\n\
         測定 ID: {}\n",
        m.id
    );
    (subject, body)
}

/// テスト送信の件名と本文 (乗務員名・測定値は入れない)
pub fn compose_test() -> (String, String) {
    let subject = format!("【テスト】{SUBJECT_PREFIX}テスト送信");
    let body = "これはアルコール検知メールのテスト送信です。\n\
                アルコール検知時には、この宛先に通知が届きます。\n"
        .to_owned();
    (subject, body)
}

/// グループの使える宛先の email を、渡された transaction の中で引く。(グループが在るか, email (昇順・重複なし))
pub(crate) async fn recipient_emails(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    group: &str,
) -> Result<(bool, Vec<String>), tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            RECIPIENTS,
            &[(&group, Type::TEXT), (&tenant_id, Type::UUID)],
        )
        .await?;
    let mut emails: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get::<_, Option<String>>(0))
        .filter(|e| !e.trim().is_empty())
        .collect();
    // 同じ email の宛先が 2 つ在っても 1 通
    emails.sort();
    emails.dedup();
    Ok((!rows.is_empty(), emails))
}

/// 宛先の email だけを、短い tenant_tx 1 回で引く (テスト送信)。(グループが在るか, 使える宛先の email)
pub async fn recipients(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
) -> Result<(bool, Vec<String>), tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            recipient_emails(tx, tenant_id, ALERT_GROUP).await
        })
    })
    .await
}

/// 宛先の email と乗務員名を、短い tenant_tx 1 回で引く。(グループが在るか, 使える宛先の email, 乗務員名)
async fn lookup(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    employee_id: Uuid,
) -> Result<(bool, Vec<String>, String), tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let (group_exists, emails) = recipient_emails(tx, tenant_id, ALERT_GROUP).await?;
            let name = tx
                .query_typed_opt(
                    EMPLOYEE_NAME,
                    &[(&employee_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?
                .map(|r| r.get::<_, String>(0));
            Ok((
                group_exists,
                emails,
                name.unwrap_or_else(|| UNKNOWN_EMPLOYEE.to_owned()),
            ))
        })
    })
    .await
}

/// 送る用意ができた通数 (送り手と、宛先ごとの 1 通)
#[derive(Clone)]
pub struct Outbox {
    mailer: SharedMailer,
    mails: Vec<AlertMail>,
}

impl Outbox {
    /// 宛先ごとに同じ件名・本文の 1 通
    pub(crate) fn new(
        mailer: SharedMailer,
        emails: Vec<String>,
        subject: &str,
        body: &str,
    ) -> Self {
        let mails = emails
            .into_iter()
            .map(|to| AlertMail {
                to,
                subject: subject.to_owned(),
                body: body.to_owned(),
                attachment: None,
            })
            .collect();
        Outbox { mailer, mails }
    }

    /// 全員の 1 通に同じ写真 (bytes は共有) を載せ、本文に [`PHOTO_NOTE`] を足す
    fn with_photo(mut self, bytes: Vec<u8>) -> Self {
        let bytes: Arc<[u8]> = bytes.into();
        for mail in &mut self.mails {
            mail.body.push_str(PHOTO_NOTE);
            mail.attachment = Some(Attachment {
                filename: "photo.jpg",
                content_type: "image/jpeg",
                bytes: bytes.clone(),
            });
        }
        self
    }
}

/// 応答を返した後に送るもの。ハンドラーが [`prepare`] で宛先を引いたあと応答の extension に入れ、Worker の fetch が
/// 取り出して `wait_until` に渡す (保存の応答を写真の読みと送信で待たせない)。持つのは送り手と宛先ごとの 1 通・写真の
/// URL と、それを読む部品 (R2・ログ) だけで、**DB には触らない** (pg の lock を持たない)
#[derive(Clone)]
pub struct PendingAlert {
    outbox: Outbox,
    tenant_id: Uuid,
    photo_url: Option<String>,
    files: Option<SharedStore>,
    log: LogSink,
}

impl PendingAlert {
    /// 送ると決まった (`outbox` が `Some`) ときだけ作る
    pub fn new(
        state: &AppState,
        tenant_id: Uuid,
        outbox: Option<Outbox>,
        photo_url: Option<&str>,
    ) -> Option<Self> {
        Some(PendingAlert {
            outbox: outbox?,
            tenant_id,
            photo_url: photo_url.map(str::to_owned),
            files: state.files.clone(),
            log: state.log.clone(),
        })
    }

    /// 測定の顔写真を R2 から読んで添付し ([`read_media`]。要求の tenant_id で照合し、別 tenant の写真は読まない)、
    /// 宛先ごとに送る ([`deliver`])。写真が無い・R2 が無い・key が不正・取れないときは本文だけで送る
    /// (取れない理由はログ 1 行・固定の語)。返すのは (送れた数, 失敗した数)
    pub async fn send(self) -> (usize, usize) {
        let PendingAlert {
            mut outbox,
            tenant_id,
            photo_url,
            files,
            log,
        } = self;
        if let (Some(url), Some(files)) = (photo_url.as_deref(), files.as_ref()) {
            let photo = read_media(&log, files, tenant_id, url, Media::FacePhoto, "alert photo");
            if let Some(bytes) = photo.await {
                outbox = outbox.with_photo(bytes);
            }
        }
        deliver(outbox, &log).await
    }
}

/// 保存が条件を満たした (`alert`) ときだけ、宛先を引いて送る用意をする。送らない理由はログ 1 行 (固定の語。
/// グループが無いときだけ別の語 = 設定漏れの見分け)。**pg の lock を持ったまま呼び**、返した `Outbox` は
/// [`PendingAlert`] に包んで応答の後に送る
pub async fn prepare(
    state: &AppState,
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    alert: bool,
    m: &Measurement,
) -> Option<Outbox> {
    if !alert {
        return None;
    }
    let Some(mailer) = state.mailer.clone() else {
        (state.log)("measurements: alert skipped: no mailer");
        return None;
    };
    let (group_exists, emails, name) = match lookup(pg, tenant_id, dev, m.employee_id).await {
        Ok(found) => found,
        Err(e) => {
            // 保存はもう commit 済み。ログだけ残して送らない
            state.db_fail("alert lookup", &e);
            return None;
        }
    };
    if !group_exists {
        (state.log)("measurements: alert skipped: no group");
        return None;
    }
    if emails.is_empty() {
        (state.log)("measurements: alert skipped: no recipients");
        return None;
    }
    let (subject, body) = compose(&name, m);
    Some(Outbox::new(mailer, emails, &subject, &body))
}

/// 宛先ごとに 1 通ずつ送る。失敗はログ 1 行 (段の名前だけ) で、ほかの宛先へは続ける。リトライしない。
/// 返すのは (送れた数, 失敗した数)
pub async fn deliver(outbox: Outbox, log: &LogSink) -> (usize, usize) {
    let (mut sent, mut failed) = (0, 0);
    for mail in &outbox.mails {
        match outbox.mailer.send(mail).await {
            Ok(()) => sent += 1,
            Err(e) => {
                failed += 1;
                log(&format!("measurements: alert send failed: {}", e.0));
            }
        }
    }
    (sent, failed)
}

/// テスト送信を許す呼び手か (auth-worker が付ける `X-User-Role`。alc-notify-worker の recipients.rs と同じ形)
pub(crate) fn is_admin(auth_user: &Option<Extension<AuthUser>>) -> bool {
    auth_user
        .as_ref()
        .is_some_and(|Extension(u)| u.role == "admin")
}

pub(crate) fn error(status: StatusCode, word: &'static str) -> Response {
    (status, Json(json!({ "error": word }))).into_response()
}

/// `POST /api/measurements/alert-test` — グループの使える宛先へ確認用の 1 通ずつを送る (測定は起こさない)。
/// admin 以外 403 `forbidden`・送り手が無い 503 `mail_unavailable`・宛先が 0 (グループが無いも含む) 409 `no_recipients`・
/// 宛先の引きの失敗 500 (本文なし)・送った 200 `{"sent", "failed"}`
pub async fn alert_test(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    if !is_admin(&auth_user) {
        return error(StatusCode::FORBIDDEN, "forbidden");
    }
    let Some(mailer) = state.mailer.clone() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "mail_unavailable");
    };
    let mut pg = state.pg.lock().await;
    let found = recipients(&mut pg, tenant_id, dev.get()).await;
    drop(pg);
    let emails = match found {
        Ok((_, emails)) => emails,
        Err(e) => return state.db_fail("alert test lookup", &e).into_response(),
    };
    if emails.is_empty() {
        return error(StatusCode::CONFLICT, "no_recipients");
    }
    let (subject, body) = compose_test();
    let (sent, failed) = deliver(Outbox::new(mailer, emails, &subject, &body), &state.log).await;
    Json(json!({ "sent": sent, "failed": failed })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    #[test]
    fn alert_only_when_the_value_rises_above_zero() {
        // 旧が NULL か 0 以下 → 新が > 0 だけ true
        assert!(should_alert(None, Some(0.01)));
        assert!(should_alert(Some(0.0), Some(0.01)));
        assert!(should_alert(Some(-1.0), Some(0.01)));
        // 新が NULL・0・負 → false
        assert!(!should_alert(None, None));
        assert!(!should_alert(None, Some(0.0)));
        assert!(!should_alert(Some(0.0), Some(-0.5)));
        assert!(!should_alert(Some(0.2), None));
        // 旧がすでに > 0 → false (>0 → >0・>0 → 0)
        assert!(!should_alert(Some(0.01), Some(0.2)));
        assert!(!should_alert(Some(0.2), Some(0.0)));
        // NaN は > 0 ではない
        assert!(!should_alert(None, Some(f64::NAN)));
    }

    #[test]
    fn compose_is_japanese_with_jst_time_and_a_single_line_subject() {
        let at: DateTime<Utc> = "2026-09-12T15:30:00Z".parse().unwrap();
        let m = Measurement {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            employee_id: Uuid::nil(),
            alcohol_level: Some(0.15),
            result: None,
            device_use_count: 0,
            face_photo_url: None,
            video_url: None,
            measured_at: at,
            created_at: at,
            updated_at: at,
            status: "completed".into(),
            temperature: None,
            systolic: None,
            diastolic: None,
            pulse: None,
            medical_measured_at: None,
            face_verified: None,
            medical_manual_input: None,
            device_id: None,
        };
        let (subject, body) = compose("山田\r\n太郎", &m);
        assert_eq!(subject, "アルコール検知: 山田  太郎");
        assert!(body.contains("乗務員: 山田  太郎\n"));
        assert!(body.contains("アルコール値: 0.15\n"));
        assert!(body.contains("測定日時: 2026-09-13 00:30:00 (日本時間)\n"));
        assert!(body.contains("測定 ID: 00000000-0000-0000-0000-000000000000\n"));
    }

    #[test]
    fn compose_test_has_no_driver_or_value() {
        let (subject, body) = compose_test();
        assert_eq!(subject, "【テスト】アルコール検知: テスト送信");
        assert_eq!(
            body,
            "これはアルコール検知メールのテスト送信です。\nアルコール検知時には、この宛先に通知が届きます。\n"
        );
    }
}
