# alc-measurements-worker

測定の Worker `alc-measurements` (Refs ippoan/alc-app#436、ippoan/rust-alc-api#697)。rust-alc-api の `crates/alc-misc` の
測定の口を移す先で、path・method・JSON の形・status は rust と同じ (運行者端末・管理画面がそのまま使う)。いまの口は
`/api/measurements` の 5 本 (`crates/measurements/src/measurements.rs`): `POST /start`・`PUT /{id}`・`POST`・`GET`・`GET /{id}`、
と顔写真・動画の 2 本 (`files.rs`): `GET /{id}/face-photo`・`GET /{id}/video` (R2 `MEASUREMENTS_R2` から読むだけ)。
印 `record_as_tenko` 付きの完了の保存は、同じ transaction で点呼セッションと点呼記録を作る (`tenko.rs`。電子車検証の照合は `carins.rs`)。
直下 = Worker (wasm 専用)、`crates/measurements/` = 口・SQL。

**呼び手は auth-worker の Service Binding だけ** (auth-worker が JWT を検証し `X-Tenant-ID` を付け直す。この Worker は
認証しない)。auth-worker の振り分け (`ALC_BINDING_ROUTES`) は別の PR。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
MEASUREMENTS_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ。URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。

## rust と違う振る舞い
- rust に無い口 `POST /api/measurements/alert-test` (アルコール検知メールのテスト送信。`alert::alert_test`)。**この口だけ role を見る**
  (`X-User-Role` から復元した `AuthUser` が admin でなければ 403)。宛先の引きと送信のループは検知と同じ関数 (`alert::recipients`・`alert::deliver`)。
- 顔写真・動画のキーは、保存した URL の中で `/{要求の tenant_id}/` が**最後に**現れる位置から後ろを取り (`files::extract_key`)、
  `{tenant}/{uuid}.jpg`・`{tenant}/blow-video/{uuid}.webm` の形に一致しなければ 500。rust は `{R2_PUBLIC_URL_BASE}/` を剥ぐが、
  その既定値に account ID が入り public repo に書けないため。**別 tenant の写真を指す URL は読めない (rust より厳しい)。**

## アルコール検知メール (`alert.rs`・`src/mail.rs`)
DB の列 `alcohol_level` が **旧が NULL か 0 以下 → 新が 0 より上** になった保存だけ、テナントの notify グループ「アルコール検知」
(`notify_groups`・`notify_recipient_groups`・`notify_recipients` の既存の表。専用の表は作らない) の使える宛先 (`enabled`・email あり) へ
**1 人 1 通**。PUT は tx の中で旧値を `FOR UPDATE` で読み、判定は UPDATE 後の値で見る (値を送らない PUT は COALESCE で旧値が残る)。
宛先は commit 後の短い tenant_tx で引き (失敗しても保存は残る)、送るのは pg の lock を放してから (`alert::prepare` → `drop(pg)` → `alert::deliver`)。
送信の失敗はログ 1 行だけで応答は変えない・リトライしない。ログにメールアドレス・tenant_id・氏名を出さない (グループ無しだけ別の語)。
送り手は `send_email` の binding `ALERT_EMAIL` (**トップと staging の両方**・`destination_address` は書かない) と変数 `NOTIFY_EMAIL_FROM`。
どちらかが無ければ送らない。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`MEASUREMENTS_HYPERDRIVE`) は本番だけ。R2 `MEASUREMENTS_R2` は本番 `alc-face-photos`・staging `alc-face-photos-staging`
  (staging が本番 bucket を指さない。R2 の await は tenant_tx の中に入れない)。`wrangler.toml` の表の順を変えない (新しい表は `[build]` より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id などの識別子・入力値 (車検証の番号など) を出さない。
  DB の失敗は `measurements: <段> failed: db (<kind>)` (kind は SQLSTATE か固定の label)。応答の本文は rust と同じく空。
- role は見ない (rust も `X-Tenant-ID` だけ。例外はテスト送信の口 1 本)。dev端末の印 `X-Device-Dev: 1` は rust と同じく `app.device_dev` に通す
  (`set_device_dev`。tenant_tx の最初の文。列 `is_dev` の既定値と RLS が dev の行を分ける。alc-migrations 153)。
  印は kit の `require_tenant_header` が書く `DeviceDevSlot` から読む (ヘッダを口で読み直さない)。
  **dev 端末の印を tenant_tx の中で立てている。kit に口ができたら置き換える。**
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
  WHERE に tenant_id も明示する。列は `SELECT *` にせず、行型の欄の順に並べる (`MEASUREMENT_COLUMNS`・`SESSION_COLUMNS`。位置で読む)。
- 点呼の記録は内側の区切り (`SAVEPOINT`。名前は `tenko.rs` の定数) で作り、失敗しても測定の保存は残す。
  **`tenko.rs` の `status_for_result`・`record_payload`・`insert_record`・`TenkoSession` は rust (alc-tenko) と 2 か所に在る**
  (rust の自動点呼が同じものを使う)。片方を変えたらもう片方も変える (列・規則・record_hash の作り方)。
  serde_json に `preserve_order` を付けない (record_data の key の順 = hash が rust と変わる)。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 16・api_db 9・files_db 14・normal_tenko_db 26・alert_db 28) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・Hyperdrive の設定の ID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-trouble-worker と同じ rev)。
