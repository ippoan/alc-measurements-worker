# alc-measurements-worker

測定の Worker `alc-measurements` (Refs ippoan/alc-app#436、ippoan/rust-alc-api#697)。rust-alc-api の `crates/alc-misc` の
測定の口を移す先で、path・method・JSON の形・status は rust と同じ (運行者端末・管理画面がそのまま使う)。いまの口は
`/api/measurements` の 5 本 (`crates/measurements/src/measurements.rs`): `POST /start`・`PUT /{id}`・`POST`・`GET`・`GET /{id}`。
印 `record_as_tenko` 付きの完了の保存は、同じ transaction で点呼セッションと点呼記録を作る (`tenko.rs`。電子車検証の照合は `carins.rs`)。
rust に残る口: 顔写真・動画 (`/api/measurements/{id}/face-photo`・`/video`。R2 と一緒に別の PR で移す)。
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

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`MEASUREMENTS_HYPERDRIVE`) は本番だけ。`wrangler.toml` の表の順を変えない (新しい表は `[build]` より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id などの識別子・入力値 (車検証の番号など) を出さない。
  DB の失敗は `measurements: <段> failed: db (<kind>)` (kind は SQLSTATE か固定の label)。応答の本文は rust と同じく空。
- role は見ない (rust も `X-Tenant-ID` だけ)。dev端末の印 `X-Device-Dev: 1` は rust と同じく `app.device_dev` に通す
  (`set_device_dev`。tenant_tx の最初の文。列 `is_dev` の既定値と RLS が dev の行を分ける。alc-migrations 153)。
  印は kit の `require_tenant_header` が書く `DeviceDevSlot` から読む (ヘッダを口で読み直さない)。
  **dev 端末の印を tenant_tx の中で立てている。kit に口ができたら置き換える。**
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
  WHERE に tenant_id も明示する。列は `SELECT *` にせず、行型の欄の順に並べる (`MEASUREMENT_COLUMNS`・`SESSION_COLUMNS`。位置で読む)。
- 点呼の記録は内側の区切り (`SAVEPOINT`。名前は `tenko.rs` の定数) で作り、失敗しても測定の保存は残す。
  **`tenko.rs` の `status_for_result`・`record_payload`・`insert_record`・`TenkoSession` は rust (alc-tenko) と 2 か所に在る**
  (rust の自動点呼が同じものを使う)。片方を変えたらもう片方も変える (列・規則・record_hash の作り方)。
  serde_json に `preserve_order` を付けない (record_data の key の順 = hash が rust と変わる)。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 9・api_db 9・normal_tenko_db 26) はテストを足したら上げる。
- public repo: ホスト名・account ID・tenant UUID・Hyperdrive の設定の ID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-trouble-worker と同じ rev)。
