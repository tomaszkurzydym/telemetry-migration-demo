//! Runs fixtures/golden.json (independently specified expectations) through the Rust pipeline.
use rand::Rng;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use telemetry_processor::crypto::{decrypt_location, LocationCipher, LocationEnc};
use telemetry_processor::pipeline::{Decision, Processor, Source};

fn synthetic_coords() -> HashMap<&'static str, (String, String)> {
    let mut r = rand::thread_rng();
    let mut m = HashMap::new();
    for t in ["@LOC-A", "@LOC-B"] {
        m.insert(
            t,
            (
                format!("{:.6}", r.gen_range(10.0..30.0)),
                format!("{:.6}", r.gen_range(-40.0..-20.0)),
            ),
        );
    }
    m.insert("@LOC-INVALID", ("95.5".to_string(), "-30.0".to_string()));
    m
}

fn num_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            Decimal::from_str(&x.to_string()).unwrap() == Decimal::from_str(&y.to_string()).unwrap()
        }
        _ => a == b,
    }
}

#[test]
fn golden_fixtures() {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fixtures/golden.json"
    ))
    .unwrap();
    let doc: Value = serde_json::from_str(&raw).unwrap();
    let cipher = LocationCipher::from_bytes(&[7u8; 32], "golden").unwrap();
    let coords = synthetic_coords();
    let mut checked = 0;
    for case in doc["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut p = Processor::new();
        for (i, rec) in case["records"].as_array().unwrap().iter().enumerate() {
            let mut input = match rec.get("input_raw") {
                Some(Value::String(s)) => s.clone(),
                _ => serde_json::to_string(&rec["input"]).unwrap(),
            };
            for (tok, (lat, lon)) in &coords {
                input = input.replace(
                    &format!("\"{tok}\""),
                    &format!("{{\"lat\":{lat},\"lon\":{lon}}}"),
                );
            }
            let key = rec["key"].as_str().unwrap();
            let src = Source {
                topic: "golden",
                partition: 0,
                offset: i as i64,
            };
            let d = p.process(Some(key.as_bytes()), input.as_bytes(), &src, &cipher);
            p.mark_committed();
            let exp = &rec["expect"];
            let ctx = format!("{name}[{i}]");
            match d {
                Decision::Emit {
                    key: out_key,
                    record,
                    ..
                } => {
                    let want = exp
                        .get("emit")
                        .unwrap_or_else(|| panic!("{ctx}: unexpected emit"));
                    assert_eq!(out_key, want["vin"].as_str().unwrap(), "{ctx}: key");
                    let obj = record.as_object().unwrap();
                    let want_obj = want.as_object().unwrap();
                    let mut keys: Vec<_> = obj.keys().filter(|k| *k != "location_enc").collect();
                    keys.sort();
                    let mut wkeys: Vec<_> = want_obj.keys().collect();
                    wkeys.sort();
                    assert_eq!(keys, wkeys, "{ctx}: key set");
                    for (k, v) in want_obj {
                        assert!(
                            num_eq(&obj[k], v),
                            "{ctx}: field {k}: got {} want {}",
                            obj[k],
                            v
                        );
                    }
                    match exp.get("location_enc").and_then(|v| v.as_str()) {
                        Some(tok) => {
                            let enc: LocationEnc =
                                serde_json::from_value(record["location_enc"].clone()).unwrap();
                            let loc = decrypt_location(
                                &cipher,
                                &out_key,
                                want["event_id"].as_str().unwrap(),
                                &enc,
                            )
                            .unwrap();
                            let (lat, lon) = &coords[tok];
                            assert_eq!(
                                Decimal::from_str(&loc.lat.to_string()).unwrap(),
                                Decimal::from_str(lat).unwrap(),
                                "{ctx}"
                            );
                            assert_eq!(
                                Decimal::from_str(&loc.lon.to_string()).unwrap(),
                                Decimal::from_str(lon).unwrap(),
                                "{ctx}"
                            );
                        }
                        None => assert!(
                            record.get("location_enc").is_none(),
                            "{ctx}: unexpected location_enc"
                        ),
                    }
                }
                Decision::Quarantine { record, code, .. } => {
                    assert_eq!(
                        Some(code.as_str()),
                        exp.get("quarantine").and_then(|v| v.as_str()),
                        "{ctx}: quarantine code"
                    );
                    let s = record.to_string();
                    for (lat, lon) in coords.values() {
                        assert!(
                            !s.contains(lat.as_str()) && !s.contains(lon.as_str()),
                            "{ctx}: plaintext coordinate in quarantine"
                        );
                    }
                    if exp.get("redacted") == Some(&Value::Bool(true)) {
                        assert!(s.contains("[REDACTED]"), "{ctx}: not redacted");
                    }
                }
                Decision::Duplicate { .. } => assert_eq!(
                    exp.get("duplicate"),
                    Some(&Value::Bool(true)),
                    "{ctx}: unexpected duplicate"
                ),
            }
            checked += 1;
        }
    }
    assert_eq!(checked, 34);
}
