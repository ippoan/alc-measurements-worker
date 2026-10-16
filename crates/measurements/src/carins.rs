//! 電子車検証の番号 (Refs ippoan/alc-app-s3#110)。rust-alc-api の `alc-core` の
//! `repository/car_inspections.rs` (`normalize_carins_numbers`) と `repo/car_inspections.rs` (`lookup_expiry`) の写し。
//! kit (`alc-core-wasm`) に同じものが無いのでここに写す。照合の SQL を変えたら rust の `LOOKUP_SQL` も揃える
//! (rust の kiosk の照合口が同じ SQL を使う)。

use alc_worker_db::TenantTx;
use chrono::NaiveDate;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

/// 番号の形が合わない (管理番号は数字 12〜13 桁、車両 ID は英数 14 桁)
#[derive(Debug, PartialEq, Eq)]
pub struct InvalidCarinsNumber;

/// 端末から受け取った電子車検証の番号を検査する。空文字は None に寄せる。
pub fn normalize_carins_numbers(
    cert_no: &mut Option<String>,
    vehicle_id: &mut Option<String>,
) -> Result<(), InvalidCarinsNumber> {
    fn check(
        value: &mut Option<String>,
        valid: fn(&str) -> bool,
    ) -> Result<(), InvalidCarinsNumber> {
        if value.as_deref() == Some("") {
            *value = None;
        }
        match value.as_deref() {
            Some(v) if !valid(v) => Err(InvalidCarinsNumber),
            _ => Ok(()),
        }
    }
    check(cert_no, |v| {
        (12..=13).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_digit())
    })?;
    check(vehicle_id, |v| {
        v.len() == 14 && v.bytes().all(|b| b.is_ascii_alphanumeric())
    })
}

/// 電子車検証の番号で car_inspection を照合した結果 (rust の `CarinsLookup` から、kiosk の照合口だけが使う
/// 登録番号 `car_no` を除いたもの)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarinsLookup {
    /// 2 次元コードの有効期限。形の崩れた値は None
    pub expires_on: Option<NaiveDate>,
    /// `cert_no` (管理番号で一致) / `car_id` (車両 ID で一致) / `none` (該当なし)
    pub matched_by: &'static str,
}

/// 元号 → 西暦のオフセット表 (元号の和暦年 + オフセット = 西暦年)。rust の `ERA_OFFSETS` と同じ
pub const ERA_OFFSETS: &[(&str, i32)] = &[("令和", 2018), ("平成", 1988)];

/// 管理番号か車両 ID のどちらかで一致する行のうち、期限の最も新しい 1 行を引く (rust の `LOOKUP_SQL` と同じ。
/// 期限の決め方・並び・`matched_by` の判定は rust の doc を参照)。
/// rust との違いは 2 つだけ: kiosk 用の `car_no` を読まないことと、RLS に加えて WHERE に `tenant_id = $5` を書くこと
pub const LOOKUP_SQL: &str = r#"
WITH era_map(era, offset_year) AS (
    SELECT * FROM unnest($3::text[], $4::int[])
)
SELECT
    COALESCE(ci."ElectCertMgNo" = $1::text, FALSE) AS by_cert_no,
    COALESCE(
        CASE
            WHEN ci."TwodimensionCodeInfoValidPeriodExpirdate"
                 ~ '^\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$'
            THEN to_date('20' || ci."TwodimensionCodeInfoValidPeriodExpirdate", 'YYYYMMDD')
        END,
        wareki.expires_on
    ) AS expires_on
FROM car_inspection ci
LEFT JOIN LATERAL (
    -- 和暦 4 列から期限を組み立てる。to_date / make_date が例外を投げないよう、
    -- 年月日はすべて事前に範囲検査してから最後に 1 回だけ make_date を呼ぶ
    SELECT fin.expires_on
    FROM (
        SELECT
            CASE WHEN ci."ValidPeriodExpirdateY" ~ '^\d{1,4}$'
                 THEN em.offset_year + ci."ValidPeriodExpirdateY"::int END AS y,
            CASE WHEN ci."ValidPeriodExpirdateM" ~ '^\d{1,2}$'
                 THEN ci."ValidPeriodExpirdateM"::int END AS m,
            CASE WHEN ci."ValidPeriodExpirdateD" ~ '^\d{1,2}$'
                 THEN ci."ValidPeriodExpirdateD"::int END AS d
        FROM era_map em
        WHERE em.era = regexp_replace(ci."ValidPeriodExpirdateE", '[[:space:]　]', '', 'g')
        LIMIT 1
    ) parsed
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN (parsed.y BETWEEN 1 AND 9999) AND (parsed.m BETWEEN 1 AND 12)
            THEN date_part(
                'day',
                make_date(parsed.y, parsed.m, 1) + interval '1 month - 1 day'
            )::int
        END AS last_day
    ) bnd
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN parsed.d BETWEEN 1 AND bnd.last_day
            THEN make_date(parsed.y, parsed.m, parsed.d)
        END AS expires_on
    ) fin
) wareki ON TRUE
WHERE (ci."ElectCertMgNo" = $1::text OR ci."CarId" = $2::text)
  AND ci.tenant_id = $5
ORDER BY expires_on DESC NULLS LAST, ci.created_at DESC
LIMIT 1
"#;

/// [`LOOKUP_SQL`] を呼び手の transaction で流す (rust の `lookup_expiry`)
pub async fn lookup_expiry(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    cert_no: Option<&str>,
    car_id: Option<&str>,
) -> Result<CarinsLookup, Error> {
    let era_names: Vec<&str> = ERA_OFFSETS.iter().map(|&(name, _)| name).collect();
    let era_offsets: Vec<i32> = ERA_OFFSETS.iter().map(|&(_, offset)| offset).collect();
    let row = tx
        .query_typed_opt(
            LOOKUP_SQL,
            &[
                (&cert_no, Type::TEXT),
                (&car_id, Type::TEXT),
                (&era_names, Type::TEXT_ARRAY),
                (&era_offsets, Type::INT4_ARRAY),
                (&tenant_id, Type::UUID),
            ],
        )
        .await?;
    Ok(match row {
        Some(r) => CarinsLookup {
            expires_on: r.get(1),
            matched_by: if r.get::<_, bool>(0) {
                "cert_no"
            } else {
                "car_id"
            },
        },
        None => CarinsLookup {
            expires_on: None,
            matched_by: "none",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize(
        cert_no: Option<&str>,
        vehicle_id: Option<&str>,
    ) -> Result<(Option<String>, Option<String>), InvalidCarinsNumber> {
        let mut c = cert_no.map(str::to_string);
        let mut v = vehicle_id.map(str::to_string);
        normalize_carins_numbers(&mut c, &mut v).map(|()| (c, v))
    }

    #[test]
    fn test_normalize_carins_numbers_accepts_valid_shapes() {
        assert_eq!(normalize(None, None), Ok((None, None)));
        assert_eq!(
            normalize(Some("000000000001"), Some("TESTCARID00001")),
            Ok((
                Some("000000000001".to_string()),
                Some("TESTCARID00001".to_string())
            ))
        );
        assert!(normalize(Some("0000000000001"), None).is_ok());
    }

    #[test]
    fn test_normalize_carins_numbers_empty_is_none() {
        assert_eq!(normalize(Some(""), Some("")), Ok((None, None)));
    }

    #[test]
    fn test_normalize_carins_numbers_rejects_bad_shapes() {
        assert!(normalize(Some("00000000001"), None).is_err());
        assert!(normalize(Some("00000000000012"), None).is_err());
        assert!(normalize(Some("00000000000A"), None).is_err());
        assert!(normalize(None, Some("TESTCARID0001")).is_err());
        assert!(normalize(None, Some("TESTCARID-0001")).is_err());
    }
}
