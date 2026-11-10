//! Validation, normalization and quarantine format (docs/contract.md §2–§7).

use regex::Regex;
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{json, Map, Value};
use std::sync::LazyLock;

pub const SCHEMA_V: i32 = 1;
pub const MAX_TS_MS: i128 = 253_402_300_799_999;
const MAX_ODOMETER_KM: i64 = 2_000_000;
const MAX_ODOMETER_SCALE: i64 = 3;
const MAX_LABOR_MIN: i128 = 1440;
const MAX_DTC_CODES: usize = 32;
const MAX_PARTS: usize = 100;
const MAX_PART_QTY: i128 = 999;
const STATUSES: [&str; 4] = ["OPEN", "IN_PROGRESS", "COMPLETED", "CANCELLED"];
const REQUIRED: [&str; 7] = [
    "vin",
    "work_order_id",
    "ts_ms",
    "status",
    "service_center_id",
    "odometer",
    "odometer_unit",
];
const NOTES_KEY: &str = "technician_notes";

static WORK_ORDER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^WO-[0-9]{8}$").unwrap());
static SERVICE_CENTER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^SC-[0-9]{3}$").unwrap());
static DTC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[PBCU][0-9A-F]{4}$").unwrap());
static PART_NO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^PN-[0-9A-Z]{6}$").unwrap());
static INTEGER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^-?[0-9]+$").unwrap());

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub work_order_id: String,
    pub vin: String,
    pub status: String,
    pub service_center_id: String,
    pub event_time_ms: i64,
    pub odometer_km: Decimal,
    pub dtc_codes: Vec<String>,
    pub parts_count: i32,
    pub labor_min: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Rejected {
    pub code: &'static str,
    pub work_order_id: Option<String>,
    pub vin: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Valid(Row),
    Rejected(Rejected),
}

pub fn parse_object(value: Option<&[u8]>) -> Option<Map<String, Value>> {
    let s = std::str::from_utf8(value?).ok()?;
    match serde_json::from_str::<Value>(s).ok()? {
        Value::Object(o) => Some(o),
        _ => None,
    }
}

fn present<'a>(o: &'a Map<String, Value>, k: &str) -> Option<&'a Value> {
    o.get(k).filter(|v| !v.is_null())
}

fn text<'a>(o: &'a Map<String, Value>, k: &str) -> Option<&'a str> {
    o.get(k).and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// Integer literal (no fraction/exponent) within lo..=hi.
fn int_in(v: &Value, lo: i128, hi: i128) -> Option<i128> {
    let Value::Number(n) = v else { return None };
    let s = n.to_string();
    if !INTEGER.is_match(&s) || s.len() > 30 {
        return None;
    }
    s.parse::<i128>().ok().filter(|i| (lo..=hi).contains(i))
}

pub fn is_work_order(s: &str) -> bool {
    WORK_ORDER.is_match(s)
}

/// Checks run in contract §3 order; the first failing check decides the error code.
pub fn validate(key: Option<&str>, value: Option<&[u8]>) -> Outcome {
    let Some(o) = parse_object(value) else {
        return Outcome::Rejected(Rejected {
            code: "MALFORMED_JSON",
            work_order_id: None,
            vin: None,
        });
    };
    let vin_raw = text(&o, "vin");
    let reject = |code: &'static str| {
        Outcome::Rejected(Rejected {
            code,
            work_order_id: text(&o, "work_order_id")
                .filter(|w| is_work_order(w))
                .map(str::to_owned),
            vin: vin_raw.map(str::to_owned),
        })
    };
    if REQUIRED.iter().any(|k| present(&o, k).is_none()) {
        return reject("MISSING_REQUIRED_FIELD");
    }
    let Some(vin) = vin_raw else {
        return reject("MISSING_REQUIRED_FIELD");
    };
    if key != Some(vin) {
        return reject("KEY_MISMATCH");
    }
    let Some(wo) = text(&o, "work_order_id").filter(|w| is_work_order(w)) else {
        return reject("INVALID_WORK_ORDER");
    };
    let Some(ts) = int_in(&o["ts_ms"], 0, MAX_TS_MS) else {
        return reject("INVALID_TS");
    };
    let Some(status) = text(&o, "status").filter(|s| STATUSES.contains(s)) else {
        return reject("INVALID_STATUS");
    };
    let Some(sc) = text(&o, "service_center_id").filter(|s| SERVICE_CENTER.is_match(s)) else {
        return reject("INVALID_SERVICE_CENTER");
    };
    let Some(unit) = text(&o, "odometer_unit").filter(|u| *u == "km" || *u == "mi") else {
        return reject("INVALID_UNIT");
    };
    let Some(km) = odometer_km(&o["odometer"], unit) else {
        return reject("INVALID_ODOMETER");
    };
    let Some(dtcs) = dtc_codes(present(&o, "dtc_codes")) else {
        return reject("INVALID_DTC");
    };
    let Some(parts) = parts_count(present(&o, "parts")) else {
        return reject("INVALID_PARTS");
    };
    let labor = match present(&o, "labor_min") {
        None => None,
        Some(v) => match int_in(v, 0, MAX_LABOR_MIN) {
            Some(l) => Some(l as i32),
            None => return reject("INVALID_LABOR"),
        },
    };
    Outcome::Valid(Row {
        work_order_id: wo.to_owned(),
        vin: vin.to_owned(),
        status: status.to_owned(),
        service_center_id: sc.to_owned(),
        event_time_ms: ts as i64,
        odometer_km: km,
        dtc_codes: dtcs,
        parts_count: parts,
        labor_min: labor,
    })
}

/// Exact decimal from a JSON number literal, bounded before any allocation: returns None unless
/// 0 <= value < 10^7 with at most MAX_ODOMETER_SCALE decimals (after dropping trailing zeros).
fn bounded_decimal(lit: &str) -> Option<Decimal> {
    let (neg, rest) = match lit.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, lit),
    };
    let (mantissa, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, "0"),
    };
    let (int_part, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: String = format!("{int_part}{frac}");
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some(Decimal::ZERO);
    }
    if neg {
        return None;
    }
    let exp_neg = exp.starts_with('-');
    let exp_digits = exp.trim_start_matches(['+', '-']).trim_start_matches('0');
    if exp_digits.len() > 15 {
        return None; // |exponent| >= 10^15: far outside range or scale either way
    }
    let e: i64 = if exp_digits.is_empty() {
        0
    } else {
        exp_digits.parse().ok()?
    };
    let mut exp10 = if exp_neg { -e } else { e } - frac.len() as i64;
    let mut digits = digits;
    while let Some(d) = digits.strip_suffix('0') {
        digits = d;
        exp10 += 1;
    }
    if -exp10 > MAX_ODOMETER_SCALE || digits.len() as i64 + exp10 > 7 {
        return None;
    }
    let mut m: i128 = digits.parse().ok()?;
    let scale = if exp10 < 0 { (-exp10) as u32 } else { 0 };
    if exp10 > 0 {
        m *= 10i128.pow(exp10 as u32);
    }
    Decimal::try_from_i128_with_scale(m, scale).ok()
}

/// Contract §4: range and scale on the raw value, convert, round half-up to 1 dp, range again.
pub fn odometer_km(v: &Value, unit: &str) -> Option<Decimal> {
    let Value::Number(n) = v else { return None };
    let d = bounded_decimal(&n.to_string())?;
    let max = Decimal::from(MAX_ODOMETER_KM);
    if d > max {
        return None;
    }
    let factor = if unit == "mi" {
        Decimal::new(1_609_344, 6)
    } else {
        Decimal::ONE
    };
    let mut km = (d * factor).round_dp_with_strategy(1, RoundingStrategy::MidpointAwayFromZero);
    km.rescale(1);
    (km <= max).then_some(km)
}

pub fn dtc_codes(v: Option<&Value>) -> Option<Vec<String>> {
    match v {
        None => Some(Vec::new()),
        Some(Value::Array(a)) if a.len() <= MAX_DTC_CODES => {
            let mut out = a
                .iter()
                .map(|e| {
                    let c = e.as_str()?.trim_matches(|c: char| c <= ' ').to_uppercase();
                    DTC.is_match(&c).then_some(c)
                })
                .collect::<Option<Vec<_>>>()?;
            out.sort();
            out.dedup();
            Some(out)
        }
        _ => None,
    }
}

pub fn parts_count(v: Option<&Value>) -> Option<i32> {
    match v {
        None => Some(0),
        Some(Value::Array(a)) if a.len() <= MAX_PARTS => a
            .iter()
            .map(|p| {
                let o = p.as_object()?;
                text(o, "part_no").filter(|n| PART_NO.is_match(n))?;
                int_in(o.get("qty")?, 1, MAX_PART_QTY).map(|q| q as i32)
            })
            .sum(),
        _ => None,
    }
}

fn redact(v: &mut Value) {
    match v {
        Value::Object(o) => {
            for (k, x) in o.iter_mut() {
                if k == NOTES_KEY {
                    *x = Value::String("[REDACTED]".into());
                } else {
                    redact(x);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact),
        _ => {}
    }
}

/// Contract §7: parseable payloads kept with technician_notes redacted at any depth; others not retained.
pub fn redact_payload(value: Option<&[u8]>) -> String {
    match parse_object(value) {
        Some(o) => {
            let mut v = Value::Object(o);
            redact(&mut v);
            v.to_string()
        }
        None => format!(
            "[MALFORMED: {} bytes, not retained]",
            value.map_or(0, <[u8]>::len)
        ),
    }
}

pub fn quarantine(
    r: &Rejected,
    value: Option<&[u8]>,
    topic: &str,
    partition: i32,
    offset: i64,
) -> String {
    json!({
        "schema_v": SCHEMA_V,
        "error_code": r.code,
        "work_order_id": r.work_order_id,
        "vin": r.vin,
        "source": {"topic": topic, "partition": partition, "offset": offset},
        "payload": redact_payload(value),
    })
    .to_string()
}

/// Failure log line: work_order_id + error_code only (contract §8).
pub fn log_line(r: &Rejected) -> String {
    format!(
        "quarantined work_order_id={} error_code={}",
        r.work_order_id.as_deref().unwrap_or("-"),
        r.code
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn km(lit: &str, unit: &str) -> Option<String> {
        let v: Value = serde_json::from_str(lit).unwrap();
        odometer_km(&v, unit).map(|d| d.to_string())
    }

    #[test]
    fn odometer_boundaries() {
        assert_eq!(km("100", "mi").as_deref(), Some("160.9"));
        assert_eq!(km("1242742.415", "mi").as_deref(), Some("2000000.0"));
        assert_eq!(km("1242742.416", "mi"), None);
        assert_eq!(km("2000000", "km").as_deref(), Some("2000000.0"));
        assert_eq!(km("2000000.001", "km"), None);
        assert_eq!(km("-0", "km").as_deref(), Some("0.0"));
        assert_eq!(km("0e99999999999999999999", "km").as_deref(), Some("0.0"));
        assert_eq!(km("1.2340", "km").as_deref(), Some("1.2"));
        assert_eq!(km("1.5e2", "km").as_deref(), Some("150.0"));
        assert_eq!(km("12.25", "km").as_deref(), Some("12.3"));
    }

    #[test]
    fn hostile_exponents_do_not_allocate_or_panic() {
        for lit in [
            "1e9999999999",
            "1e-9999999999",
            "1e99999999999999999999999",
            "1e-99999999999999999999999",
            "9e6",
            "1e7",
        ] {
            assert_eq!(km(lit, "km"), None, "{lit}");
        }
    }

    #[test]
    fn redaction_and_log_line_never_carry_notes() {
        let raw = br#"{"status":"x","technician_notes":"SYN-NOTE-1","a":[{"technician_notes":{"t":"SYN-NOTE-2"}}]}"#;
        let r = match validate(Some("k"), Some(raw)) {
            Outcome::Rejected(r) => r,
            Outcome::Valid(_) => unreachable!(),
        };
        let q = quarantine(&r, Some(raw), "t", 0, 1);
        assert!(!q.contains("SYN-NOTE"));
        assert!(!log_line(&r).contains("SYN-NOTE"));
    }
}
