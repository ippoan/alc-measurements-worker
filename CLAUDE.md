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
- rust に無い点呼の確定の後回し (`deferred.rs`): 完了 PUT に `record_as_tenko` と `defer_tenko: true` が付けば、記録を作らず入力を
  `measurements.tenko_pending` に置く (alc-migrations 180)。確定は `POST /api/measurements/{id}/tenko` (`通常点呼`/`IT点呼`) か、
  定時実行 (`[triggers]` 1 分ごと) の見回りが完了 (`updated_at`) から 2 分後に `通常点呼` で。見回りの一覧は SECURITY DEFINER の
  `list_pending_tenko` を nil の tenant_tx で引き、1 件ずつその tenant と dev の軸で確定する。
- 顔写真・動画のキーは、保存した URL の中で `/{要求の tenant_id}/` が**最後に**現れる位置から後ろを取り (`files::extract_key`)、
  `{tenant}/{uuid}.jpg`・`{tenant}/blow-video/{uuid}.webm` の形に一致しなければ 500。rust は `{R2_PUBLIC_URL_BASE}/` を剥ぐが、
  その既定値に account ID が入り public repo に書けないため。**別 tenant の写真を指す URL は読めない (rust より厳しい)。**

## アルコール検知メール (`alert.rs`・`src/mail.rs`)
DB の列 `alcohol_level` が **旧が NULL か 0 以下 → 新が 0 より上** になった保存だけ、テナントの notify グループ「アルコール検知」
(`notify_groups`・`notify_recipient_groups`・`notify_recipients` の既存の表。専用の表は作らない) の使える宛先 (`enabled`・email あり) へ
**1 人 1 通**。PUT は tx の中で旧値を `FOR UPDATE` で読み、判定は UPDATE 後の値で見る (値を送らない PUT は COALESCE で旧値が残る)。
宛先は commit 後の短い tenant_tx で引き (失敗しても保存は残る。`alert::prepare`)、顔写真の R2 読みと送信は**応答を返した後**:
口が `alert::PendingAlert` (Outbox・tenant_id・写真の URL・R2・ログ。DB は持たない) を応答の extension に入れ、`src/lib.rs` の fetch が
取り出して `ctx.wait_until` で `PendingAlert::send` (写真の添付 → `alert::deliver`) を走らせる (応答の本文・ヘッダーは変えない)。
native のテストは `tests/support` の `call_raw` が同じく取り出して await する (`respond_raw` は取り出さずに返す)。テスト送信の口は今も同期で送る。
送信の失敗はログ 1 行だけで応答は変えない・リトライしない。ログにメールアドレス・tenant_id・氏名を出さない (グループ無しだけ別の語)。
送り手は `send_email` の binding `ALERT_EMAIL` (**トップと staging の両方**・`destination_address` は書かない) と変数 `NOTIFY_EMAIL_FROM`。
どちらかが無ければ送らない。

## 血圧の通知 (`bp.rs`・`bp_settings.rs`・`report.rs`・`service.rs`)
**宛先と受け取りの設定は専用の表 `bp_notify_settings`** (alc-migrations 186。1 社員 1 行: `lineworks_user_id`・`lineworks_name`・`email`・
`notify_self`・`daily_report`。(tenant, lineworks_user_id) は部分 UNIQUE)。**既存の通知の宛先 `notify_recipients`・notify のグループは血圧の通知では一切使わない**
(「血圧の日報」「血圧の日報を受け取らない」グループは残っていても無視)。
- 本人への LINE WORKS: 完了の PUT で systolic が**初めて**入った保存 (旧 systolic が NULL。tx の中で `FOR UPDATE` の旧値と比べる) の後、
  `bp_notify_settings` の `employee_id` = 測定の乗務員・`notify_self`・`lineworks_user_id` あり (`bp::sql::RECIPIENTS`) へ。基準値は
  `employee_health_baselines` (無ければ 120/80 ±10)、外れたら「基準から外れています」を足す。`bp::PendingBp` を応答の extension に入れ、
  `src/lib.rs` が `ctx.wait_until` で送る (アルコール検知メールと同じ形)。dev 端末の要求・宛先なし・送信の失敗でも保存は成功。
  送り先は alc-lineworks-worker の Service Binding `ALC_LINEWORKS` (本番 `alc-lineworks`・staging `alc-lineworks-staging`)。ログは固定の語と件数だけ。
- 毎朝 7:00 (日本時間) の日報メール: 本番の cron `0 22 * * *` (`report::DAILY_CRON` と同じ文字列。staging の cron は見回りだけ)。
  テナントを `tenants` から引き、**管理者**へ前日 (日本時間 0〜24 時) の一覧を 1 人 1 通 (既定で送り、外したい人だけ外すオプトアウト)。
  宛先 (`report::sql::RECIPIENTS`) = 削除済みでない社員 (`deleted_at IS NULL`) の `role` (TEXT[]) に `manager` か `admin` を含み、
  `bp_notify_settings` の `daily_report` が true で `email` がある人。**設定の行が無い管理者は email が無いので送らない。**
  アルコール検知メールの宛先 (`alert::recipient_emails`・グループ「アルコール検知」) とは別の引き。テナントごとに失敗しても続ける。
- 設定の口 (`bp_settings.rs`。管理画面 alc-app が呼ぶ。**4 口とも admin だけ**: `X-User-Role` が admin でなければ 403 `forbidden`。`alert::is_admin` と同じ):
  `GET /api/bp-notify/settings` (削除済みでない全社員を社員コード順に、設定を LEFT JOIN)・`PUT /api/bp-notify/settings/{employee_id}`
  (来ていない欄はそのまま・`null` は空・同じ lineworks_user_id が他の社員に在れば 409 `lineworks_user_taken`・社員が居なければ 404・email 不正は 400)・
  `GET /api/bp-notify/lineworks-users` (Service Binding の `GET /api/internal/lineworks/users`。取れなければ 502、binding 無しは 503 `lineworks_unavailable`)・
  `POST /api/bp-notify/link-by-name` (`{dry_run}` 省略で true。LINE WORKS のメンバー全体と、lineworks_user_id が無い社員を空白 (半角・全角) を除いた氏名で比べ、
  一致がちょうど 1 人で他の社員に結び付いていなければ候補 (`bp_settings::plan`)。確定は 1 transaction、先を越されたら何も書かず 409)。
  `Service` trait は `post` と `get` (応答は status と本文)。**メンバー一覧の await は tenant_tx の外**。ログに氏名・LINE WORKS の id・メールを出さない。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`MEASUREMENTS_HYPERDRIVE`) は本番だけ。R2 `MEASUREMENTS_R2` は本番 `alc-face-photos`・staging `alc-face-photos-staging`
  (staging が本番 bucket を指さない。R2 の await は tenant_tx の中に入れない)。`wrangler.toml` の表の順を変えない (新しい表は `[build]` より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id などの識別子・入力値 (車検証の番号など) を出さない。
  DB の失敗は `measurements: <段> failed: db (<kind>)` (kind は SQLSTATE か固定の label)。応答の本文は rust と同じく空。
- role は見ない (rust も `X-Tenant-ID` だけ。例外はテスト送信の口と血圧の通知の設定の 4 口)。dev端末の印 `X-Device-Dev: 1` は rust と同じく `app.device_dev` に通す
  (`set_device_dev`。tenant_tx の最初の文。列 `is_dev` の既定値と RLS が dev の行を分ける。alc-migrations 153)。
  印は kit の `require_tenant_header` が書く `DeviceDevSlot` から読む (ヘッダを口で読み直さない)。
  **dev 端末の印を tenant_tx の中で立てている。kit に口ができたら置き換える。**
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
  WHERE に tenant_id も明示する。列は `SELECT *` にせず、行型の欄の順に並べる (`MEASUREMENT_COLUMNS`・`SESSION_COLUMNS`。位置で読む)。
- 点呼の記録は内側の区切り (`SAVEPOINT`。名前は `tenko.rs` の定数) で作り、失敗しても測定の保存は残す。
  **`tenko.rs` の `status_for_result`・`record_payload`・`insert_record`・`TenkoSession` は rust (alc-tenko) と 2 か所に在る**
  (rust の自動点呼が同じものを使う)。片方を変えたらもう片方も変える (列・規則・record_hash の作り方)。
  serde_json に `preserve_order` を付けない (record_data の key の順 = hash が rust と変わる)。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 35・api_db 9・files_db 14・normal_tenko_db 26・alert_db 29・deferred_tenko_db 13・bp_db 10・report_db 12・bp_settings_db 18) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・Hyperdrive の設定の ID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-trouble-worker と同じ rev)。
