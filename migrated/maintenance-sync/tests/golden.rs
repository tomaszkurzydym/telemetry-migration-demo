//! Runs fixtures/golden.json (hand-specified from docs/contract.md) through the Rust port.
use maintenance_sync::contract::{quarantine, validate, Outcome, Row};
use maintenance_sync::db::{self, ApplyError, DbConfig, OffsetMove};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

fn golden() -> Value {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/golden.json");
    serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
}

fn value_bytes(c: &Value) -> Option<Vec<u8>> {
    if let Some(h) = c["value_hex"].as_str() {
        return Some(
            (0..h.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
                .collect(),
        );
    }
    c["value"].as_str().map(|s| s.as_bytes().to_vec())
}

fn row_json(r: &Row) -> Value {
    serde_json::json!({"work_order_id": r.work_order_id, "vin": r.vin, "status": r.status,
        "service_center_id": r.service_center_id, "event_time_ms": r.event_time_ms,
        "odometer_km": r.odometer_km.to_string(), "dtc_codes": r.dtc_codes, "parts_count": r.parts_count,
        "labor_min": r.labor_min})
}

/// JSON equality with numbers compared by value.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.to_string().parse::<f64>().ok() == y.to_string().parse::<f64>().ok()
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

#[test]
fn golden_cases() {
    let g = golden();
    let cases = g["cases"].as_array().unwrap();
    assert!(cases.len() >= 60);
    let mut failures = Vec::new();
    for c in cases {
        let name = c["name"].as_str().unwrap();
        let e = &c["expect"];
        let bytes = value_bytes(c);
        let got = validate(c["key"].as_str(), bytes.as_deref());
        let ok = match (&got, e.get("row"), e.get("reject")) {
            (Outcome::Valid(r), Some(er), _) => same(&row_json(r), er),
            (Outcome::Rejected(r), _, Some(eq)) => {
                let q: Value = serde_json::from_str(&quarantine(
                    r,
                    bytes.as_deref(),
                    "maintenance.events.raw",
                    0,
                    0,
                ))
                .unwrap();
                let payload = q["payload"].as_str().unwrap();
                r.code == eq["error_code"]
                    && q["work_order_id"] == eq["work_order_id"]
                    && q["vin"] == eq["vin"]
                    && e.get("payload").is_none_or(|p| p == payload)
                    && e.get("payload_json")
                        .is_none_or(|p| same(p, &serde_json::from_str(payload).unwrap()))
                    && !q.to_string().contains("SYN-NOTE")
            }
            _ => false,
        };
        if !ok {
            failures.push(format!("{name}: got {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn db_config() -> Option<DbConfig> {
    let mut env: HashMap<String, String> =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env"))
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .filter_map(|l| {
                l.split_once('=')
                    .map(|(k, v)| (k.trim().to_owned(), v.to_owned()))
            })
            .collect();
    env.extend(std::env::vars());
    let cfg = DbConfig::from_env(&env).ok()?;
    cfg.connect().ok().map(|_| cfg)
}

#[test]
fn golden_sequences_against_service_db() {
    let Some(cfg) = db_config() else {
        eprintln!("SKIPPED: service-db not configured or not reachable");
        return;
    };
    let mut c = cfg.connect().unwrap();
    c.batch_execute("CREATE TEMP TABLE golden_rows (LIKE service_records INCLUDING ALL)")
        .unwrap();
    for s in golden()["sequences"].as_array().unwrap() {
        c.batch_execute("TRUNCATE golden_rows").unwrap();
        let rows: Vec<Row> = s["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| {
                let raw = e.as_str().unwrap();
                let key: Value = serde_json::from_str(raw).unwrap();
                match validate(key["vin"].as_str(), Some(raw.as_bytes())) {
                    Outcome::Valid(r) => Some(r),
                    _ => None,
                }
            })
            .collect();
        // One event per transaction, as if each arrived in its own batch.
        for r in &rows {
            db::apply_batch(
                &mut c,
                "golden_rows",
                "golden",
                "t",
                &[],
                std::slice::from_ref(r),
            )
            .unwrap();
        }
        let got: BTreeMap<String, Value> = c.query(
            "SELECT work_order_id, vin, status, service_center_id, (extract(epoch from event_time)*1000)::bigint, \
             odometer_km::text, dtc_codes, parts_count, labor_min FROM golden_rows", &[]).unwrap().iter().map(|r| {
            (r.get::<_, String>(0), serde_json::json!({"work_order_id": r.get::<_, String>(0), "vin": r.get::<_, String>(1),
                "status": r.get::<_, String>(2), "service_center_id": r.get::<_, String>(3), "event_time_ms": r.get::<_, i64>(4),
                "odometer_km": r.get::<_, String>(5), "dtc_codes": r.get::<_, Vec<String>>(6), "parts_count": r.get::<_, i32>(7),
                "labor_min": r.get::<_, Option<i32>>(8)}))
        }).collect();
        let want: BTreeMap<String, Value> = s["expect"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(got, want, "{}", s["name"]);
    }
}

#[test]
fn offset_store_is_fenced() {
    let Some(cfg) = db_config() else {
        eprintln!("SKIPPED: service-db not configured or not reachable");
        return;
    };
    let mut c = cfg.connect().unwrap();
    let group = format!("golden-fence-{}", std::process::id());
    let mv = |prev, next| OffsetMove {
        partition: 0,
        prev,
        next,
    };
    db::apply_batch(
        &mut c,
        "service_records_shadow",
        &group,
        "t",
        &[mv(None, 5)],
        &[],
    )
    .unwrap();
    assert!(matches!(
        db::apply_batch(
            &mut c,
            "service_records_shadow",
            &group,
            "t",
            &[mv(None, 9)],
            &[]
        ),
        Err(ApplyError::Fenced(0))
    ));
    assert!(matches!(
        db::apply_batch(
            &mut c,
            "service_records_shadow",
            &group,
            "t",
            &[mv(Some(4), 9)],
            &[]
        ),
        Err(ApplyError::Fenced(0))
    ));
    db::apply_batch(
        &mut c,
        "service_records_shadow",
        &group,
        "t",
        &[mv(Some(5), 9)],
        &[],
    )
    .unwrap();
    assert_eq!(
        db::load_offsets(&mut c, &group, "t").unwrap().get(&0),
        Some(&9)
    );
    c.execute(
        "DELETE FROM maintenance_shadow_offsets WHERE consumer_group = $1",
        &[&group],
    )
    .unwrap();
}
