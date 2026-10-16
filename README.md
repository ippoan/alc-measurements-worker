# alc-measurements-worker

アルコール測定 (measurements) の Cloudflare Worker `alc-measurements`。rust-alc-api の `crates/alc-misc` の測定の口から
分割する (Refs ippoan/alc-app#436、ippoan/rust-alc-api#697)。型は ippoan/alc-trouble-worker と同じ。
アルコール検知のメール通知 (ippoan/alc-app#436) はこの Worker に後から足す。

## 口

全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401)。path・method・JSON の形・status は
rust-alc-api と同じ。

| path | method | 内容 |
|---|---|---|
| `/api/measurements/start` | POST | `{"employee_id"}` だけで status = `started` の測定を作る (201) |
| `/api/measurements/{id}` | PUT | 来た欄だけ変える (200)。結果・状態・点呼の種別・点呼方法・本人確認の方法・電子車検証の番号の検証は 400、無ければ 404。応答は測定の欄 + `tenko_session_id` |
| `/api/measurements` | POST | 完了済みの測定を 1 件作る (201。オフラインの送り直し) |
| `/api/measurements` | GET | 絞り込み (`employee_id`・`result_type`/`result`・`date_from`/`from`・`date_to`/`to`・`status`) と `measured_at` の新しい順のページ (`per_page` 既定 50・上限 100、`page` 既定 1・下限 1)。応答は `{measurements, total, page, per_page}` |
| `/api/measurements/{id}` | GET | 1 件 (無ければ 404) |

顔写真・動画の口 (`/api/measurements/{id}/face-photo`・`/video`) はまだ rust に在る (R2 と一緒に別の PR で移す)。

**通常点呼の記録**: 保存の本文に `record_as_tenko: true` があり、保存の後の状態が `completed` なら、同じ transaction で
点呼セッションと点呼記録を 1 組作る (rust と同じ)。結果 `fail` / `over` は中止 (中止理由「アルコール検知」)、`pass` / `normal` は完了、
`error` は記録しない。同じ測定・同じ乗務員の同じ測定時刻への再送は 1 組のまま (先勝ち)。電子車検証の番号があれば車検期限を照合して写す
(照合に失敗しても記録は作る)。記録の作成に失敗しても測定の保存は残る (内側の SAVEPOINT だけ戻す)。
PUT は点呼方法 (`通常点呼` / `IT点呼`) と本人確認の方法を受け、応答の `tenko_session_id` に作った (または既に在る) セッションの id を載せる。
POST は点呼方法・本人確認の方法を受けない (通常点呼・記録なし)。

dev端末の要求 (`X-Device-Dev: 1`) の行は dev の行になり、dev の要求からだけ見える (rust と同じ。alc-migrations 153)。

## 構成

- 直下 (`src/`) — Worker 本体 (wasm 専用)。DB への経路 (`src/db.rs`: staging = Workers VPC → PgBouncer、
  本番 = Hyperdrive `MEASUREMENTS_HYPERDRIVE`、ローカル = `DATABASE_URL`) と workers-rs への載せ方
- `crates/measurements/` — 口と SQL (native でもビルドでき、`tests/api_db.rs`・`tests/normal_tenko_db.rs` が実 DB で通す)
- `scripts/` — 公開範囲の検査 (`check-exposure.sh` と陰性対照)、migration の取得、coverage の gate

デプロイ: PR → `wrangler deploy --dry-run`、main への merge → staging、タグ `v*` (Tag Release を手動) → 本番。
