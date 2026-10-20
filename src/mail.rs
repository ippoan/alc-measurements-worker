//! アルコール検知メールの送り手 ([`measurements::alert::AlertMailer`]) の Workers での実装: Cloudflare の `send_email` の
//! binding `ALERT_EMAIL` (構造化の `Email` を `send_with_builder` で送る。写真は `attachments` に載せる。MIME は手で組まない)。
//!
//! 差出人は変数 `NOTIFY_EMAIL_FROM`。宛先 (`to`) は notify のグループの email で、Cloudflare アカウントで確認済みの
//! 送り先だけが届く (binding に `destination_address` を書かない = 確認済みの全員に送れる形)。
//!
//! `AlertMailer` は `Send + Sync` で `Send` な future を返す。`SendEmail` と future は JS の値で `Send` でないので、
//! `worker::send` の `SendWrapper`・`SendFuture` で包む ([`crate::r2::R2Store`] と同じ。Workers は単一 thread)。
//!
//! エラーは段の名前だけを持つ `MailError` に落とす。**宛先・本文・ランタイムの生のエラー文はログに出さない。**
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use futures_util::future::BoxFuture;
use measurements::alert::{AlertMail, AlertMailer, MailError};
use worker::js_sys::Uint8Array;
use worker::send::{SendFuture, SendWrapper};
use worker::{EmailAttachment, SendEmail, SendEmailBuilder};

/// `send_email` の binding と差出人
pub struct EmailSender {
    binding: SendWrapper<SendEmail>,
    from: String,
}

impl EmailSender {
    pub fn new(binding: SendEmail, from: String) -> Self {
        Self {
            binding: SendWrapper::new(binding),
            from,
        }
    }
}

impl AlertMailer for EmailSender {
    fn send<'a>(&'a self, mail: &'a AlertMail) -> BoxFuture<'a, Result<(), MailError>> {
        Box::pin(SendFuture::new(async move {
            let builder =
                SendEmailBuilder::builder(&self.from, &mail.to, &mail.subject).text(&mail.body);
            let email = match &mail.attachment {
                Some(a) => {
                    let content = Uint8Array::from(&a.bytes[..]);
                    let attachment = EmailAttachment::builder_attachment_with_typed_array(
                        a.filename,
                        a.content_type,
                        &content,
                    )
                    .build();
                    builder.attachments(&[attachment]).build()
                }
                None => builder.build(),
            };
            self.binding
                .send_with_builder(&email)
                .await
                .map(|_| ())
                .map_err(|_| MailError("send"))
        }))
    }
}
