//! 血圧の通知の設定 (Refs ippoan/alc-app#436)。測った本人への LINE WORKS ([`crate::bp`]) と毎朝の管理者への日報メール
//! ([`crate::report`]) の宛先を、既存の通知の宛先 (`notify_recipients`・notify のグループ) とは**別に**、専用の表
//! `bp_notify_settings` (alc-migrations 186。1 社員 1 行) で持つ。管理画面 (alc-app) がこの口を呼ぶ。
//!
//! | 口 | 内容 |
//! |---|---|
//! | `GET /api/bp-notify/settings` | `{employees:[{employee_id, name, code, role, lineworks_user_id, lineworks_name, email, notify_self, daily_report}]}`。削除済みでない全社員を社員コード順に、設定の行を LEFT JOIN (行が無ければ lineworks 系・email は null、notify_self / daily_report は true) |
//! | `PUT /api/bp-notify/settings/{employee_id}` | 本文 (どれも省略可) `{lineworks_user_id, lineworks_name, email, notify_self, daily_report}` を upsert。省略 = そのまま・`null` = 空にする (真偽値の `null` は省略と同じ)。応答は更新後の 1 件 (GET の要素と同じ形)。他の社員が同じ lineworks_user_id → 409 `lineworks_user_taken`・社員が居ない (削除済みも) → 404 `employee_not_found`・email が不正 → 400 `invalid_email` |
//! | `GET /api/bp-notify/lineworks-users` | `{users:[{user_id, name, email}]}`。alc-lineworks-worker のメンバー一覧。取れない → 502 (binding 無しは 503) `{error:"lineworks_unavailable", message}` |
//! | `POST /api/bp-notify/link-by-name` | 本文 `{dry_run}` (省略で true) → `{dry_run, candidates, skipped, linked}`。LINE WORKS のメンバー全体と、まだ lineworks_user_id が無い社員を、空白 (半角・全角) を除いた氏名で比べる ([`plan`]) |
//!
//! **4 口とも admin だけ** (`X-User-Role` が admin でなければ 403 `forbidden`。テナントは `X-Tenant-ID`)。
//! ログは固定の語と DB の kind / status だけ (氏名・LINE WORKS の id・メールアドレスを出さない)。

use std::collections::{HashMap, HashSet};

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{kind, TenantTx, TxOutput};
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Row;
use uuid::Uuid;

use crate::alert::{error, is_admin};
use crate::service::{ServiceRequest, Unreachable};
use crate::AppState;

/// alc-lineworks-worker のメンバー一覧の口 (alc-notify-worker の `GET /api/notify/lineworks/users` と同じ呼び方。
/// `X-Tenant-ID` を付けると tenant の既定の bot で引く)
pub const LW_USERS_PATH: &str = "/api/internal/lineworks/users";

/// 候補にしなかった理由 (`skipped[].reason`)
pub const NO_LINEWORKS_USER: &str = "no_lineworks_user";
pub const MULTIPLE_LINEWORKS_USERS: &str = "multiple_lineworks_users";
pub const LINEWORKS_USER_TAKEN: &str = "lineworks_user_taken";

/// メンバー一覧が取れないときの `message` (固定。上流の文を載せない)
const UNAVAILABLE_MESSAGE: &str = "LINE WORKS のメンバー一覧を取得できませんでした";

/// 一意索引 (tenant, lineworks_user_id) の違反の SQLSTATE
const UNIQUE_VIOLATION: &str = "23505";

pub mod sql {
    /// 社員と設定 (社員コード順。コードが無い社員は最後)。$1 = tenant_id、$2 = 1 人に絞る社員 (NULL = 全員)。
    /// 行が無い社員は lineworks 系・email が NULL、notify_self / daily_report は TRUE。WHERE に tenant_id も明示する
    pub const LIST: &str =
        "SELECT e.id, e.name, e.code, e.role, s.lineworks_user_id, s.lineworks_name, s.email, \
                COALESCE(s.notify_self, TRUE), COALESCE(s.daily_report, TRUE) \
         FROM employees e \
         LEFT JOIN bp_notify_settings s ON s.tenant_id = e.tenant_id AND s.employee_id = e.id \
         WHERE e.tenant_id = $1 AND e.deleted_at IS NULL AND ($2::uuid IS NULL OR e.id = $2) \
         ORDER BY e.code NULLS LAST, e.name, e.id";

    /// 設定の表で使われている LINE WORKS の user_id (削除済みの社員の行も含む。一意索引が見るのと同じ範囲)
    pub const TAKEN: &str = "SELECT lineworks_user_id FROM bp_notify_settings \
         WHERE tenant_id = $1 AND lineworks_user_id IS NOT NULL";

    /// 1 件の upsert。$3・$5・$7 = その欄が本文に在ったか (在れば $4・$6・$8 の値、NULL なら空にする。無ければそのまま)。
    /// $9・$10 = 真偽値 (NULL なら既存の値、行を作るときは TRUE)
    pub const UPSERT: &str = "INSERT INTO bp_notify_settings \
             (tenant_id, employee_id, lineworks_user_id, lineworks_name, email, notify_self, daily_report) \
         VALUES ($1, $2, CASE WHEN $3 THEN $4 END, CASE WHEN $5 THEN $6 END, CASE WHEN $7 THEN $8 END, \
                 COALESCE($9, TRUE), COALESCE($10, TRUE)) \
         ON CONFLICT (tenant_id, employee_id) DO UPDATE SET \
             lineworks_user_id = CASE WHEN $3 THEN $4 ELSE bp_notify_settings.lineworks_user_id END, \
             lineworks_name = CASE WHEN $5 THEN $6 ELSE bp_notify_settings.lineworks_name END, \
             email = CASE WHEN $7 THEN $8 ELSE bp_notify_settings.email END, \
             notify_self = COALESCE($9, bp_notify_settings.notify_self), \
             daily_report = COALESCE($10, bp_notify_settings.daily_report), \
             updated_at = NOW()";

    /// 氏名で結び付ける 1 件の upsert。notify_self / daily_report は既存の値か既定。メンバーに email が無ければ既存の email を残す
    pub const LINK: &str = "INSERT INTO bp_notify_settings \
             (tenant_id, employee_id, lineworks_user_id, lineworks_name, email) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (tenant_id, employee_id) DO UPDATE SET \
             lineworks_user_id = EXCLUDED.lineworks_user_id, \
             lineworks_name = EXCLUDED.lineworks_name, \
             email = COALESCE(EXCLUDED.email, bp_notify_settings.email), \
             updated_at = NOW()";
}

/// 社員 1 人分の設定 (GET の要素・PUT の応答)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EmployeeSetting {
    pub employee_id: Uuid,
    pub name: String,
    pub code: Option<String>,
    pub role: Vec<String>,
    pub lineworks_user_id: Option<String>,
    pub lineworks_name: Option<String>,
    pub email: Option<String>,
    pub notify_self: bool,
    pub daily_report: bool,
}

impl TxOutput for EmployeeSetting {}

impl EmployeeSetting {
    fn from_row(r: &Row) -> Self {
        EmployeeSetting {
            employee_id: r.get(0),
            name: r.get(1),
            code: r.get(2),
            role: r.get(3),
            lineworks_user_id: r.get(4),
            lineworks_name: r.get(5),
            email: r.get(6),
            notify_self: r.get(7),
            daily_report: r.get(8),
        }
    }
}

/// LINE WORKS のメンバー 1 人 (lineworks-worker の users の 1 行)
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Member {
    pub user_id: String,
    pub user_name: Option<String>,
    pub email: Option<String>,
}

/// 氏名での結び付けの候補
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Candidate {
    pub employee_id: Uuid,
    pub employee_name: String,
    pub lineworks_user_id: String,
    pub lineworks_name: Option<String>,
    pub email: Option<String>,
}

/// 候補にしなかった社員とその理由
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Skipped {
    pub employee_id: Uuid,
    pub employee_name: String,
    pub reason: &'static str,
}

/// 空白 (半角・全角を含む `char::is_whitespace`) を全部除いた氏名
fn squash(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// まだ lineworks_user_id が無い社員を、LINE WORKS のメンバー全体と氏名で比べる。`taken` は設定の表で既に使われている
/// user_id (他の社員に結び付いているメンバーは候補にしない)。
/// 社員ごとに: 一致が 0 人 → `no_lineworks_user` (氏名が空白だけでも)、2 人以上 → `multiple_lineworks_users`、
/// ちょうど 1 人でそのメンバーが `taken` → `lineworks_user_taken`、それ以外は候補。
/// 同じメンバーを 2 人以上の社員が候補にしたとき (同姓同名) は、どの社員のものか決められないので全員 `multiple_lineworks_users`。
/// 並びは社員の並び順 (社員コード順) のまま
pub fn plan(
    employees: &[EmployeeSetting],
    taken: &HashSet<String>,
    members: &[Member],
) -> (Vec<Candidate>, Vec<Skipped>) {
    let mut found: Vec<(&EmployeeSetting, Result<&Member, &'static str>)> = Vec::new();
    for e in employees.iter().filter(|e| e.lineworks_user_id.is_none()) {
        let key = squash(&e.name);
        let same: Vec<&Member> = members
            .iter()
            .filter(|m| !key.is_empty() && m.user_name.as_deref().is_some_and(|n| squash(n) == key))
            .collect();
        let verdict = match same.as_slice() {
            [] => Err(NO_LINEWORKS_USER),
            [m] if taken.contains(&m.user_id) => Err(LINEWORKS_USER_TAKEN),
            [m] => Ok(*m),
            _ => Err(MULTIPLE_LINEWORKS_USERS),
        };
        found.push((e, verdict));
    }
    let mut claims: HashMap<&str, usize> = HashMap::new();
    for (_, verdict) in &found {
        if let Ok(m) = verdict {
            *claims.entry(m.user_id.as_str()).or_default() += 1;
        }
    }
    let (mut candidates, mut skipped) = (Vec::new(), Vec::new());
    for (e, verdict) in found {
        match verdict {
            Ok(m) if claims[m.user_id.as_str()] == 1 => candidates.push(Candidate {
                employee_id: e.employee_id,
                employee_name: e.name.clone(),
                lineworks_user_id: m.user_id.clone(),
                lineworks_name: m.user_name.clone(),
                email: m.email.clone(),
            }),
            other => skipped.push(Skipped {
                employee_id: e.employee_id,
                employee_name: e.name.clone(),
                reason: other.err().unwrap_or(MULTIPLE_LINEWORKS_USERS),
            }),
        }
    }
    (candidates, skipped)
}

/// 本文の欄が「来ていない (None)」「null (Some(None))」「値 (Some(Some))」を区別して読む
fn present<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

/// `PUT /settings/{employee_id}` の本文
#[derive(Debug, Default, Deserialize)]
struct PutBody {
    #[serde(default, deserialize_with = "present")]
    lineworks_user_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "present")]
    lineworks_name: Option<Option<String>>,
    #[serde(default, deserialize_with = "present")]
    email: Option<Option<String>>,
    notify_self: Option<bool>,
    daily_report: Option<bool>,
}

/// `POST /link-by-name` の本文
#[derive(Debug, Default, Deserialize)]
struct LinkBody {
    dry_run: Option<bool>,
}

/// 本文を読む。空なら既定 (全部省略)、JSON として読めなければ `None`
fn parse_body<T: DeserializeOwned + Default>(bytes: &Bytes) -> Option<T> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Some(T::default());
    }
    serde_json::from_slice(bytes).ok()
}

/// 前後の空白を除き、空なら null にする
fn clean(v: Option<Option<String>>) -> Option<Option<String>> {
    v.map(|v| v.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()))
}

/// メールアドレスとして送れる形か (`@` を含み、空白・制御文字を含まない。ヘッダに入るので改行を通さない)
fn valid_email(s: &str) -> bool {
    s.contains('@') && !s.chars().any(|c| c.is_whitespace() || c.is_control())
}

fn forbidden() -> Response {
    error(StatusCode::FORBIDDEN, "forbidden")
}

fn invalid_body() -> Response {
    error(StatusCode::BAD_REQUEST, "invalid_body")
}

fn taken() -> Response {
    error(StatusCode::CONFLICT, LINEWORKS_USER_TAKEN)
}

fn unavailable(status: StatusCode) -> Response {
    (
        status,
        Json(json!({"error": "lineworks_unavailable", "message": UNAVAILABLE_MESSAGE})),
    )
        .into_response()
}

/// 社員と設定を引く (`only` があればその 1 人)
async fn fetch(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    only: Option<Uuid>,
) -> Result<Vec<EmployeeSetting>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(sql::LIST, &[(&tenant_id, Type::UUID), (&only, Type::UUID)])
        .await?;
    Ok(rows.iter().map(EmployeeSetting::from_row).collect())
}

/// LINE WORKS のメンバー全体を引く。**DB の transaction の外で呼ぶ** (相手の応答待ちで transaction を延ばさない)。
/// 引けなければ返す応答 (binding 無し 503・届かない / 断られた / 読めない 502)
async fn members(state: &AppState, tenant_id: Uuid) -> Result<Vec<Member>, Response> {
    let Some(service) = state.lineworks.clone() else {
        (state.log)("measurements: bp settings failed: lineworks (no binding)");
        return Err(unavailable(StatusCode::SERVICE_UNAVAILABLE));
    };
    let req = ServiceRequest {
        path: LW_USERS_PATH.to_owned(),
        headers: vec![("X-Tenant-ID", tenant_id.to_string())],
        body: String::new(),
    };
    let fail = |why: &str| {
        (state.log)(&format!(
            "measurements: bp settings failed: lineworks ({why})"
        ));
        unavailable(StatusCode::BAD_GATEWAY)
    };
    let resp = service
        .get(req)
        .await
        .map_err(|Unreachable(label)| fail(label))?;
    if !(200..300).contains(&resp.status) {
        return Err(fail(&format!("status {}", resp.status)));
    }
    serde_json::from_slice(&resp.body).map_err(|_| fail("body"))
}

/// `GET /api/bp-notify/settings`
pub async fn list_settings(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let mut pg = state.pg.lock().await;
    let found = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move { fetch(tx, tenant_id, None).await })
        })
        .await;
    drop(pg);
    match found {
        Ok(employees) => Json(json!({ "employees": employees })).into_response(),
        Err(e) => state.db_fail("bp settings list", &e).into_response(),
    }
}

/// `PUT /api/bp-notify/settings/{employee_id}`
pub async fn put_setting(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(employee_id): Path<Uuid>,
    body: Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let Some(body) = parse_body::<PutBody>(&body) else {
        return invalid_body();
    };
    let (user_id, name, email) = (
        clean(body.lineworks_user_id),
        clean(body.lineworks_name),
        clean(body.email),
    );
    if email
        .as_ref()
        .and_then(|e| e.as_deref())
        .is_some_and(|e| !valid_email(e))
    {
        return error(StatusCode::BAD_REQUEST, "invalid_email");
    }
    let (notify_self, daily_report) = (body.notify_self, body.daily_report);
    let mut pg = state.pg.lock().await;
    let saved = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                if fetch(tx, tenant_id, Some(employee_id)).await?.is_empty() {
                    return Ok(None);
                }
                let (set_user, set_name, set_email) =
                    (user_id.is_some(), name.is_some(), email.is_some());
                let (user_id, name, email) = (user_id.flatten(), name.flatten(), email.flatten());
                let args: [(&(dyn ToSql + Sync), Type); 10] = [
                    (&tenant_id, Type::UUID),
                    (&employee_id, Type::UUID),
                    (&set_user, Type::BOOL),
                    (&user_id, Type::TEXT),
                    (&set_name, Type::BOOL),
                    (&name, Type::TEXT),
                    (&set_email, Type::BOOL),
                    (&email, Type::TEXT),
                    (&notify_self, Type::BOOL),
                    (&daily_report, Type::BOOL),
                ];
                tx.execute_typed(sql::UPSERT, &args).await?;
                Ok(fetch(tx, tenant_id, Some(employee_id)).await?.pop())
            })
        })
        .await;
    drop(pg);
    match saved {
        Ok(Some(setting)) => Json(setting).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "employee_not_found"),
        Err(e) if kind(&e) == UNIQUE_VIOLATION => taken(),
        Err(e) => state.db_fail("bp settings put", &e).into_response(),
    }
}

/// `GET /api/bp-notify/lineworks-users`
pub async fn lineworks_users(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    match members(&state, tenant_id).await {
        Ok(found) => {
            let users: Vec<_> = found
                .into_iter()
                .map(|m| json!({"user_id": m.user_id, "name": m.user_name, "email": m.email}))
                .collect();
            Json(json!({ "users": users })).into_response()
        }
        Err(resp) => resp,
    }
}

/// 氏名での結び付けの結果 (transaction の外へ持ち出す)
struct Linked {
    candidates: Vec<Candidate>,
    skipped: Vec<Skipped>,
    linked: usize,
}

impl TxOutput for Linked {}

/// `POST /api/bp-notify/link-by-name`
pub async fn link_by_name(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    body: Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let Some(body) = parse_body::<LinkBody>(&body) else {
        return invalid_body();
    };
    let dry_run = body.dry_run.unwrap_or(true);
    let members = match members(&state, tenant_id).await {
        Ok(found) => found,
        Err(resp) => return resp,
    };
    let mut pg = state.pg.lock().await;
    // 読む → 決める → 書く を 1 transaction で。先を越されたら (一意索引の違反) 何も書かずに 409
    let done = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let employees = fetch(tx, tenant_id, None).await?;
                let used = tx
                    .query_typed(sql::TAKEN, &[(&tenant_id, Type::UUID)])
                    .await?
                    .iter()
                    .map(|r| r.get::<_, String>(0))
                    .collect::<HashSet<_>>();
                let (candidates, skipped) = plan(&employees, &used, &members);
                let mut linked = 0;
                if !dry_run {
                    for c in &candidates {
                        let args: [(&(dyn ToSql + Sync), Type); 5] = [
                            (&tenant_id, Type::UUID),
                            (&c.employee_id, Type::UUID),
                            (&c.lineworks_user_id, Type::TEXT),
                            (&c.lineworks_name, Type::TEXT),
                            (&c.email, Type::TEXT),
                        ];
                        tx.execute_typed(sql::LINK, &args).await?;
                        linked += 1;
                    }
                }
                Ok(Linked {
                    candidates,
                    skipped,
                    linked,
                })
            })
        })
        .await;
    drop(pg);
    match done {
        Ok(l) => Json(json!({
            "dry_run": dry_run,
            "candidates": l.candidates,
            "skipped": l.skipped,
            "linked": l.linked,
        }))
        .into_response(),
        Err(e) if kind(&e) == UNIQUE_VIOLATION => taken(),
        Err(e) => state.db_fail("bp settings link", &e).into_response(),
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/bp-notify/settings", get(list_settings))
        .route("/bp-notify/settings/{employee_id}", put(put_setting))
        .route("/bp-notify/lineworks-users", get(lineworks_users))
        .route("/bp-notify/link-by-name", post(link_by_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emp(n: u128, name: &str, linked: Option<&str>) -> EmployeeSetting {
        EmployeeSetting {
            employee_id: Uuid::from_u128(n),
            name: name.into(),
            code: None,
            role: vec![],
            lineworks_user_id: linked.map(str::to_owned),
            lineworks_name: None,
            email: None,
            notify_self: true,
            daily_report: true,
        }
    }

    fn member(id: &str, name: Option<&str>) -> Member {
        Member {
            user_id: id.into(),
            user_name: name.map(str::to_owned),
            email: Some(format!("{id}@example.invalid")),
        }
    }

    fn reasons(skipped: &[Skipped]) -> Vec<(u128, &'static str)> {
        skipped
            .iter()
            .map(|s| (s.employee_id.as_u128(), s.reason))
            .collect()
    }

    #[test]
    fn plan_matches_names_ignoring_half_and_full_width_spaces() {
        let employees = [emp(1, "山田 太郎", None), emp(2, "鈴木　花子", None)];
        let members = [
            member("u1", Some("山田太郎")),
            member("u2", Some(" 鈴木 花 子 ")),
        ];
        let (c, s) = plan(&employees, &HashSet::new(), &members);
        assert!(s.is_empty());
        assert_eq!(
            c,
            [
                Candidate {
                    employee_id: Uuid::from_u128(1),
                    employee_name: "山田 太郎".into(),
                    lineworks_user_id: "u1".into(),
                    lineworks_name: Some("山田太郎".into()),
                    email: Some("u1@example.invalid".into()),
                },
                Candidate {
                    employee_id: Uuid::from_u128(2),
                    employee_name: "鈴木　花子".into(),
                    lineworks_user_id: "u2".into(),
                    lineworks_name: Some(" 鈴木 花 子 ".into()),
                    email: Some("u2@example.invalid".into()),
                },
            ]
        );
    }

    #[test]
    fn plan_skips_with_the_reason() {
        let employees = [
            emp(1, "一致なし", None),
            emp(2, "二人", None),
            emp(3, "使用済み", None),
            emp(4, "  ", None),
            emp(5, "結び付き済み", Some("u9")),
            emp(6, "名無し", None),
        ];
        let members = [
            member("a1", Some("二 人")),
            member("a2", Some("二人")),
            member("t1", Some("使用済み")),
            member("n1", None),
            member("w1", Some("  ")),
            member("x1", Some("結び付き済み")),
        ];
        let taken = HashSet::from(["t1".to_owned()]);
        let (c, s) = plan(&employees, &taken, &members);
        // 結び付き済みの社員は対象外 (候補にも skipped にも出ない)
        assert!(c.is_empty());
        assert_eq!(
            reasons(&s),
            [
                (1, NO_LINEWORKS_USER),
                (2, MULTIPLE_LINEWORKS_USERS),
                (3, LINEWORKS_USER_TAKEN),
                // 氏名が空白だけの社員は、名前が空白だけのメンバーとも一致させない
                (4, NO_LINEWORKS_USER),
                (6, NO_LINEWORKS_USER),
            ]
        );
    }

    #[test]
    fn plan_does_not_give_one_member_to_two_employees() {
        let employees = [emp(1, "同 姓同名", None), emp(2, "同姓 同名", None)];
        let members = [member("u1", Some("同姓同名"))];
        let (c, s) = plan(&employees, &HashSet::new(), &members);
        assert!(c.is_empty());
        assert_eq!(
            reasons(&s),
            [(1, MULTIPLE_LINEWORKS_USERS), (2, MULTIPLE_LINEWORKS_USERS)]
        );
    }

    #[test]
    fn body_fields_tell_absent_from_null() {
        let b: PutBody =
            serde_json::from_str(r#"{"email": null, "lineworks_name": "x", "notify_self": false}"#)
                .unwrap();
        assert_eq!(b.email, Some(None));
        assert_eq!(b.lineworks_name, Some(Some("x".into())));
        assert_eq!(b.lineworks_user_id, None);
        assert_eq!(b.notify_self, Some(false));
        assert_eq!(b.daily_report, None);
    }

    #[test]
    fn parse_body_reads_empty_as_default_and_rejects_garbage() {
        assert!(parse_body::<PutBody>(&Bytes::new()).is_some());
        assert!(parse_body::<PutBody>(&Bytes::from_static(b" \n")).is_some());
        assert!(parse_body::<PutBody>(&Bytes::from_static(b"{")).is_none());
        assert!(parse_body::<PutBody>(&Bytes::from_static(b"[]")).is_none());
        assert_eq!(
            parse_body::<LinkBody>(&Bytes::from_static(b"{}"))
                .unwrap()
                .dry_run,
            None
        );
    }

    #[test]
    fn clean_trims_and_empties_to_null() {
        assert_eq!(clean(None), None);
        assert_eq!(clean(Some(None)), Some(None));
        assert_eq!(clean(Some(Some("  ".into()))), Some(None));
        assert_eq!(clean(Some(Some(" a b ".into()))), Some(Some("a b".into())));
    }

    #[test]
    fn email_needs_an_at_and_no_whitespace_or_control() {
        assert!(valid_email("a@example.invalid"));
        assert!(!valid_email("no-at"));
        assert!(!valid_email("a b@example.invalid"));
        assert!(!valid_email("a@example.invalid\nBcc: x@example.invalid"));
        assert!(!valid_email("a@example\u{0}.invalid"));
    }
}
