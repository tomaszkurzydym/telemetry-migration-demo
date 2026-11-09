//! Quarantine payload redaction (contract §7) and log sanitization (§9).

use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

pub const REDACTED: &str = "[REDACTED]";

fn location_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#""location"\s*:\s*(\{[^}]*\}?|"(?:[^"\\]|\\.)*"?|[^,}\]]*)"#).unwrap()
    })
}

fn latlon_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#""(lat|lon|latitude|longitude)"\s*:\s*-?[0-9][0-9.eE+\-]*"#).unwrap()
    })
}

/// Text-level redaction for payloads that are not a parseable JSON object.
pub fn redact_raw_text(s: &str) -> String {
    let s = location_re().replace_all(s, format!("\"location\":\"{REDACTED}\"").as_str());
    latlon_re()
        .replace_all(&s, format!("\"$1\":\"{REDACTED}\"").as_str())
        .into_owned()
}

const LOCATION_KEYS: [&str; 5] = ["location", "lat", "lon", "latitude", "longitude"];

fn redact_tree(v: &mut Value) {
    match v {
        Value::Object(m) => {
            for (k, val) in m.iter_mut() {
                if LOCATION_KEYS.contains(&k.as_str()) {
                    *val = Value::String(REDACTED.into());
                } else {
                    redact_tree(val);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact_tree),
        _ => {}
    }
}

/// Returns the original payload with every location-like key, at any depth, redacted.
pub fn redact_payload(value: &[u8]) -> String {
    if let Ok(mut v @ Value::Object(_)) = serde_json::from_slice::<Value>(value) {
        redact_tree(&mut v);
        return serde_json::to_string(&v).expect("re-serializing parsed JSON cannot fail");
    }
    redact_raw_text(&String::from_utf8_lossy(value))
}

/// Log-safe event_id: allowed charset only, max 64 chars.
pub fn sanitize_event_id(id: Option<&str>) -> String {
    match id {
        None => "-".into(),
        Some(s) => s
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
            .take(64)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parseable_object_location_replaced_order_kept() {
        let out =
            redact_payload(br#"{"vin":"v","location":{"lat":10.12345,"lon":-29.5},"soc_pct":101}"#);
        assert_eq!(out, r#"{"vin":"v","location":"[REDACTED]","soc_pct":101}"#);
    }

    #[test]
    fn parseable_object_without_location_unchanged() {
        assert_eq!(
            redact_payload(br#"{"vin":"v","soc_pct":1.50}"#),
            r#"{"vin":"v","soc_pct":1.50}"#
        );
    }

    #[test]
    fn invalid_location_values_are_still_redacted() {
        let out = redact_payload(br#"{"location":{"lat":10.12345,"lon":"east"}}"#);
        assert_eq!(out, r#"{"location":"[REDACTED]"}"#);
        assert_eq!(
            redact_payload(br#"{"location":"somewhere"}"#),
            r#"{"location":"[REDACTED]"}"#
        );
    }

    #[test]
    fn nested_and_stray_coordinates_redacted_in_parsed_objects() {
        let out = redact_payload(
            br#"{"vin":"v","lat":10.5,"extra":{"longitude":-29.5,"pts":[{"location":{"lat":1,"lon":2}}]}}"#,
        );
        assert_eq!(
            out,
            r#"{"vin":"v","lat":"[REDACTED]","extra":{"longitude":"[REDACTED]","pts":[{"location":"[REDACTED]"}]}}"#
        );
    }

    #[test]
    fn truncated_payload_redacted() {
        let out = redact_payload(br#"{"vin":"v","location":{"lat":10.12345,"lo"#);
        assert_eq!(out, r#"{"vin":"v","location":"[REDACTED]""#);
        assert!(!out.contains("10.12345"));
    }

    #[test]
    fn nan_and_array_payloads_redacted() {
        let out = redact_payload(br#"{"soc_pct":NaN,"location":{"lat":10.12345,"lon":-29.12345}}"#);
        assert_eq!(out, r#"{"soc_pct":NaN,"location":"[REDACTED]"}"#);
        let out = redact_payload(br#"[{"location":{"lat":10.12345,"lon":-29.12345}}]"#);
        assert_eq!(out, r#"[{"location":"[REDACTED]"}]"#);
    }

    #[test]
    fn stray_lat_lon_redacted() {
        let out = redact_raw_text(r#"{"x":{"lat": -10.5e1,"lon":3}"#);
        assert_eq!(out, r#"{"x":{"lat":"[REDACTED]","lon":"[REDACTED]"}"#);
    }

    #[test]
    fn sanitize() {
        assert_eq!(sanitize_event_id(Some("evt-1 {\"lat\":1}")), "evt-1lat:1");
        assert_eq!(sanitize_event_id(None), "-");
        assert_eq!(sanitize_event_id(Some(&"a".repeat(100))).len(), 64);
    }
}
