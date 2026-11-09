//! Validation and normalization (contract §2–§4). Pure functions, no I/O.

use crate::crypto::Location;
use chrono::DateTime;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{Map, Number, Value};
use std::str::FromStr;

pub const SCHEMA_V: u64 = 2;
pub const MAX_TS_MS: i128 = 253_402_300_799_999;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorCode {
    MalformedJson,
    MissingRequiredField,
    InvalidType,
    KeyMismatch,
    InvalidTs,
    OutOfRangeSoc,
    OutOfRangeTemp,
    InvalidLocation,
}

impl ErrorCode {
    pub const ALL: [ErrorCode; 8] = [
        ErrorCode::MalformedJson,
        ErrorCode::MissingRequiredField,
        ErrorCode::InvalidType,
        ErrorCode::KeyMismatch,
        ErrorCode::InvalidTs,
        ErrorCode::OutOfRangeSoc,
        ErrorCode::OutOfRangeTemp,
        ErrorCode::InvalidLocation,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::MalformedJson => "MALFORMED_JSON",
            ErrorCode::MissingRequiredField => "MISSING_REQUIRED_FIELD",
            ErrorCode::InvalidType => "INVALID_TYPE",
            ErrorCode::KeyMismatch => "KEY_MISMATCH",
            ErrorCode::InvalidTs => "INVALID_TS",
            ErrorCode::OutOfRangeSoc => "OUT_OF_RANGE_SOC",
            ErrorCode::OutOfRangeTemp => "OUT_OF_RANGE_TEMP",
            ErrorCode::InvalidLocation => "INVALID_LOCATION",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidEvent {
    pub vin: String,
    pub event_id: String,
    pub ts_ms: i64,
    pub soc_pct: Option<Decimal>,
    pub pack_voltage_v: Option<Number>,
    pub pack_temp_c: Option<Decimal>,
    pub cell_delta_mv: Option<Decimal>,
    pub firmware: Option<String>,
    pub location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Rejection {
    pub code: ErrorCode,
    pub vin: Option<String>,
    pub event_id: Option<String>,
}

/// Exact decimal from a JSON number literal (arbitrary_precision keeps the source text).
pub fn decimal_of(n: &Number) -> Option<Decimal> {
    let s = n.to_string();
    if s.contains(['e', 'E']) {
        Decimal::from_scientific(&s).ok()
    } else {
        Decimal::from_str_exact(&s).ok()
    }
}

fn is_integer_literal(n: &Number) -> bool {
    let s = n.to_string();
    let digits = s.strip_prefix('-').unwrap_or(&s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

fn present<'a>(obj: &'a Map<String, Value>, k: &str) -> Option<&'a Value> {
    match obj.get(k) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v),
    }
}

fn str_field(obj: &Map<String, Value>, k: &str) -> Option<String> {
    match obj.get(k) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

pub fn parse_object(value: &[u8]) -> Option<Map<String, Value>> {
    match serde_json::from_slice::<Value>(value) {
        Ok(Value::Object(m)) => Some(m),
        _ => None,
    }
}

pub fn validate(key: Option<&[u8]>, value: &[u8]) -> Result<ValidEvent, Rejection> {
    let obj = match parse_object(value) {
        Some(m) => m,
        None => {
            return Err(Rejection {
                code: ErrorCode::MalformedJson,
                vin: None,
                event_id: None,
            })
        }
    };
    let reject = |code| Rejection {
        code,
        vin: str_field(&obj, "vin"),
        event_id: str_field(&obj, "event_id"),
    };

    // §3.2: presence + type, in field order.
    let mut vin = String::new();
    let mut event_id = String::new();
    for k in ["vin", "event_id"] {
        match obj.get(k) {
            None | Some(Value::Null) => return Err(reject(ErrorCode::MissingRequiredField)),
            Some(Value::String(s)) if s.is_empty() => {
                return Err(reject(ErrorCode::MissingRequiredField))
            }
            Some(Value::String(s)) => {
                if k == "vin" {
                    vin = s.clone()
                } else {
                    event_id = s.clone()
                }
            }
            Some(_) => return Err(reject(ErrorCode::InvalidType)),
        }
    }
    let ts_raw: i128 = match obj.get("ts_ms") {
        None | Some(Value::Null) => return Err(reject(ErrorCode::MissingRequiredField)),
        Some(Value::String(s)) if s.is_empty() => {
            return Err(reject(ErrorCode::MissingRequiredField))
        }
        Some(Value::Number(n)) if is_integer_literal(n) => {
            // Arbitrarily long integer literals are integers (type ok); out-of-range handled in §3.4.
            n.to_string()
                .parse::<i128>()
                .unwrap_or(if n.to_string().starts_with('-') {
                    i128::MIN
                } else {
                    i128::MAX
                })
        }
        Some(_) => return Err(reject(ErrorCode::InvalidType)),
    };
    let mut nums: [Option<Number>; 4] = [None, None, None, None];
    for (i, k) in ["soc_pct", "pack_voltage_v", "pack_temp_c", "cell_delta_mv"]
        .iter()
        .enumerate()
    {
        match present(&obj, k) {
            None => {}
            Some(Value::Number(n)) => {
                if decimal_of(n).is_none() {
                    return Err(reject(ErrorCode::InvalidType));
                }
                nums[i] = Some(n.clone())
            }
            Some(_) => return Err(reject(ErrorCode::InvalidType)),
        }
    }
    let firmware = match present(&obj, "firmware") {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(reject(ErrorCode::InvalidType)),
    };
    let location = match present(&obj, "location") {
        None => None,
        Some(Value::Object(l)) => match (l.get("lat"), l.get("lon")) {
            (Some(Value::Number(lat)), Some(Value::Number(lon)))
                if decimal_of(lat).is_some() && decimal_of(lon).is_some() =>
            {
                Some(Location {
                    lat: lat.clone(),
                    lon: lon.clone(),
                })
            }
            _ => return Err(reject(ErrorCode::InvalidLocation)),
        },
        Some(_) => return Err(reject(ErrorCode::InvalidLocation)),
    };

    // §3.3 key check
    if key != Some(vin.as_bytes()) {
        return Err(reject(ErrorCode::KeyMismatch));
    }
    // §3.4 ts range
    if !(0..=MAX_TS_MS).contains(&ts_raw) {
        return Err(reject(ErrorCode::InvalidTs));
    }
    let soc = nums[0].as_ref().and_then(decimal_of);
    if let Some(s) = soc {
        if s < Decimal::ZERO || s > Decimal::ONE_HUNDRED {
            return Err(reject(ErrorCode::OutOfRangeSoc));
        }
    }
    let temp = nums[2].as_ref().and_then(decimal_of);
    if let Some(t) = temp {
        if t < Decimal::from(-40) || t > Decimal::from(80) {
            return Err(reject(ErrorCode::OutOfRangeTemp));
        }
    }
    if let Some(l) = &location {
        let lat = decimal_of(&l.lat).unwrap();
        let lon = decimal_of(&l.lon).unwrap();
        if lat < Decimal::from(-90)
            || lat > Decimal::from(90)
            || lon < Decimal::from(-180)
            || lon > Decimal::from(180)
        {
            return Err(reject(ErrorCode::InvalidLocation));
        }
    }
    Ok(ValidEvent {
        vin,
        event_id,
        ts_ms: ts_raw as i64,
        soc_pct: soc,
        pack_voltage_v: nums[1].clone(),
        pack_temp_c: temp,
        cell_delta_mv: nums[3].as_ref().and_then(decimal_of),
        firmware,
        location,
    })
}

pub fn round_half_up(d: Decimal, dp: u32) -> Decimal {
    d.round_dp_with_strategy(dp, RoundingStrategy::MidpointAwayFromZero)
}

pub fn event_time(ts_ms: i64) -> String {
    DateTime::from_timestamp_millis(ts_ms)
        .expect("ts_ms validated to 0..=9999-12-31")
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn dec_value(d: Decimal) -> Value {
    Value::Number(
        Number::from_str(&d.normalize().to_string()).expect("Decimal renders as a JSON number"),
    )
}

fn opt_dec(d: Option<Decimal>) -> Value {
    d.map(dec_value).unwrap_or(Value::Null)
}

/// Builds the schema_v 2 record. `location_enc` is supplied by the caller (encryption has I/O-free
/// but non-deterministic output, so it is kept out of this pure function).
pub fn normalize(ev: &ValidEvent, out_of_order: bool, location_enc: Option<Value>) -> Value {
    let mut m = Map::new();
    m.insert("schema_v".into(), Value::from(SCHEMA_V));
    m.insert("vin".into(), Value::String(ev.vin.clone()));
    m.insert("event_id".into(), Value::String(ev.event_id.clone()));
    m.insert("event_time".into(), Value::String(event_time(ev.ts_ms)));
    m.insert(
        "soc_pct".into(),
        opt_dec(ev.soc_pct.map(|s| round_half_up(s, 1))),
    );
    m.insert(
        "pack_voltage_v".into(),
        ev.pack_voltage_v
            .clone()
            .map(Value::Number)
            .unwrap_or(Value::Null),
    );
    m.insert(
        "pack_temp_k".into(),
        opt_dec(
            ev.pack_temp_c
                .map(|c| round_half_up(c + Decimal::from_str("273.15").unwrap(), 2)),
        ),
    );
    m.insert(
        "cell_delta_mv".into(),
        opt_dec(ev.cell_delta_mv.map(|d| round_half_up(d, 0))),
    );
    m.insert(
        "firmware".into(),
        ev.firmware
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert("out_of_order".into(), Value::Bool(out_of_order));
    if let Some(enc) = location_enc {
        m.insert("location_enc".into(), enc);
    }
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: &[u8] = b"SYN-VIN-0001";

    fn base() -> Map<String, Value> {
        let v: Value = serde_json::from_str(
            r#"{"vin":"SYN-VIN-0001","event_id":"e1","ts_ms":1767225600000,"soc_pct":55.25,
                "pack_voltage_v":396.40,"pack_temp_c":21.37,"cell_delta_mv":12.7,"firmware":"bms-4.2.1"}"#,
        )
        .unwrap();
        v.as_object().unwrap().clone()
    }

    fn with(k: &str, raw: &str) -> Vec<u8> {
        let mut m = base();
        m.insert(k.into(), serde_json::from_str(raw).unwrap());
        serde_json::to_vec(&Value::Object(m)).unwrap()
    }

    fn without(k: &str) -> Vec<u8> {
        let mut m = base();
        m.shift_remove(k);
        serde_json::to_vec(&Value::Object(m)).unwrap()
    }

    fn code(r: Result<ValidEvent, Rejection>) -> Option<&'static str> {
        r.err().map(|e| e.code.as_str())
    }

    #[test]
    fn soc_boundaries_inclusive() {
        for ok in ["0", "0.0", "100", "100.00", "0.0001", "99.9999"] {
            assert!(validate(Some(K), &with("soc_pct", ok)).is_ok(), "{ok}");
        }
        for bad in ["-0.01", "100.01", "100.0000001", "-1", "1e3"] {
            assert_eq!(
                code(validate(Some(K), &with("soc_pct", bad))),
                Some("OUT_OF_RANGE_SOC"),
                "{bad}"
            );
        }
    }

    #[test]
    fn temp_boundaries_inclusive() {
        for ok in ["-40", "-40.00", "80", "80.0", "0"] {
            assert!(validate(Some(K), &with("pack_temp_c", ok)).is_ok(), "{ok}");
        }
        for bad in ["-40.01", "80.001", "-273.15", "1000"] {
            assert_eq!(
                code(validate(Some(K), &with("pack_temp_c", bad))),
                Some("OUT_OF_RANGE_TEMP"),
                "{bad}"
            );
        }
    }

    #[test]
    fn required_fields() {
        for k in ["vin", "event_id", "ts_ms"] {
            assert_eq!(
                code(validate(Some(K), &without(k))),
                Some("MISSING_REQUIRED_FIELD"),
                "{k}"
            );
            assert_eq!(
                code(validate(Some(K), &with(k, "null"))),
                Some("MISSING_REQUIRED_FIELD"),
                "{k} null"
            );
            assert_eq!(
                code(validate(Some(K), &with(k, "\"\""))),
                Some("MISSING_REQUIRED_FIELD"),
                "{k} empty"
            );
        }
        // optional fields may be absent or null
        for k in [
            "soc_pct",
            "pack_voltage_v",
            "pack_temp_c",
            "cell_delta_mv",
            "firmware",
        ] {
            assert!(validate(Some(K), &without(k)).is_ok());
            assert!(validate(Some(K), &with(k, "null")).is_ok());
        }
    }

    #[test]
    fn type_checks_no_coercion() {
        assert_eq!(
            code(validate(Some(K), &with("soc_pct", "\"55\""))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("pack_temp_c", "true"))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("firmware", "4"))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("vin", "7"))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("ts_ms", "1767225600000.0"))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("ts_ms", "1.7e12"))),
            Some("INVALID_TYPE")
        );
        assert_eq!(
            code(validate(Some(K), &with("ts_ms", "\"1767225600000\""))),
            Some("INVALID_TYPE")
        );
    }

    #[test]
    fn ts_range() {
        assert!(validate(Some(K), &with("ts_ms", "0")).is_ok());
        assert!(validate(Some(K), &with("ts_ms", "253402300799999")).is_ok());
        assert_eq!(
            code(validate(Some(K), &with("ts_ms", "-1"))),
            Some("INVALID_TS")
        );
        assert_eq!(
            code(validate(Some(K), &with("ts_ms", "253402300800000"))),
            Some("INVALID_TS")
        );
        assert_eq!(
            code(validate(
                Some(K),
                &with("ts_ms", "99999999999999999999999999")
            )),
            Some("INVALID_TS")
        );
    }

    #[test]
    fn malformed_json() {
        for raw in [
            &b"{"[..],
            b"not json",
            b"[1,2]",
            b"42",
            b"{\"soc_pct\":NaN}",
            b"\xff\xfe",
            b"",
        ] {
            assert_eq!(code(validate(Some(K), raw)), Some("MALFORMED_JSON"));
        }
    }

    #[test]
    fn key_mismatch_and_precedence() {
        assert_eq!(
            code(validate(Some(b"SYN-VIN-0002"), &with("soc_pct", "500"))),
            Some("KEY_MISMATCH")
        );
        assert_eq!(code(validate(None, &base_bytes())), Some("KEY_MISMATCH"));
        // type errors win over key mismatch (field checks come first)
        assert_eq!(
            code(validate(Some(b"x"), &with("soc_pct", "\"1\""))),
            Some("INVALID_TYPE")
        );
    }

    fn base_bytes() -> Vec<u8> {
        serde_json::to_vec(&Value::Object(base())).unwrap()
    }

    #[test]
    fn location_rules() {
        assert!(validate(Some(K), &with("location", r#"{"lat":90,"lon":-180}"#)).is_ok());
        assert!(validate(Some(K), &with("location", "null")).is_ok());
        for bad in [
            r#"{"lat":90.0001,"lon":0}"#,
            r#"{"lat":0,"lon":180.5}"#,
            r#"{"lat":"0","lon":0}"#,
            r#"{"lat":0}"#,
            r#""x""#,
            "[]",
        ] {
            assert_eq!(
                code(validate(Some(K), &with("location", bad))),
                Some("INVALID_LOCATION"),
                "{bad}"
            );
        }
    }

    #[test]
    fn rejection_carries_ids_when_strings() {
        let r = validate(Some(K), &with("soc_pct", "101")).unwrap_err();
        assert_eq!(r.vin.as_deref(), Some("SYN-VIN-0001"));
        assert_eq!(r.event_id.as_deref(), Some("e1"));
        let r = validate(Some(K), &with("event_id", "5")).unwrap_err();
        assert_eq!(r.event_id, None);
    }

    #[test]
    fn half_up_rounding_on_exact_decimals() {
        let d = |s: &str| Decimal::from_str(s).unwrap();
        assert_eq!(round_half_up(d("55.25"), 1), d("55.3"));
        assert_eq!(round_half_up(d("55.24"), 1), d("55.2"));
        assert_eq!(round_half_up(d("55.35"), 1), d("55.4")); // 55.35 is not exact in binary float
        assert_eq!(round_half_up(d("99.95"), 1), d("100.0"));
        assert_eq!(round_half_up(d("12.5"), 0), d("13"));
        assert_eq!(round_half_up(d("12.4"), 0), d("12"));
        assert_eq!(round_half_up(d("-39.995") + d("273.15"), 2), d("233.16"));
        assert_eq!(round_half_up(d("-0.5"), 0), d("-1"));
    }

    #[test]
    fn event_time_format() {
        assert_eq!(event_time(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(event_time(1767225600000), "2026-01-01T00:00:00.000Z");
        assert_eq!(event_time(1767225600007), "2026-01-01T00:00:00.007Z");
        assert_eq!(event_time(253402300799999), "9999-12-31T23:59:59.999Z");
    }

    #[test]
    fn normalize_shape() {
        let ev = validate(Some(K), &base_bytes()).unwrap();
        let out = normalize(&ev, false, None);
        let keys: Vec<&str> = out
            .as_object()
            .unwrap()
            .keys()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(
            keys,
            [
                "schema_v",
                "vin",
                "event_id",
                "event_time",
                "soc_pct",
                "pack_voltage_v",
                "pack_temp_k",
                "cell_delta_mv",
                "firmware",
                "out_of_order"
            ]
        );
        assert_eq!(out["soc_pct"].to_string(), "55.3");
        assert_eq!(out["pack_temp_k"].to_string(), "294.52");
        assert_eq!(out["cell_delta_mv"].to_string(), "13");
        assert_eq!(out["pack_voltage_v"].to_string(), "396.40");
        assert_eq!(out["event_time"], "2026-01-01T00:00:00.000Z");
    }
}
