//! Stateful per-VIN processing (contract §6) with a transactional overlay:
//! changes made while a Kafka transaction is open live in `dirty` until `mark_committed`,
//! and `abort` discards them, so in-memory state always matches the committed changelog.

use crate::contract::{self, ErrorCode};
use crate::crypto::{encrypt_location, LocationCipher};
use crate::redact;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, VecDeque};

pub const DEDUPE_WINDOW: usize = 1000;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VinState {
    pub max_ts_ms: i64,
    pub recent_event_ids: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct Source<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Emit {
        key: String,
        record: Value,
        out_of_order: bool,
    },
    Quarantine {
        key: Option<Vec<u8>>,
        record: Value,
        code: ErrorCode,
        event_id: Option<String>,
    },
    Duplicate {
        event_id: String,
    },
}

#[derive(Default)]
pub struct Processor {
    committed: HashMap<String, VinState>,
    dirty: HashMap<String, (i32, VinState)>,
}

impl Processor {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self, vin: &str) -> Option<&VinState> {
        self.dirty
            .get(vin)
            .map(|(_, s)| s)
            .or_else(|| self.committed.get(vin))
    }

    pub fn process(
        &mut self,
        key: Option<&[u8]>,
        value: &[u8],
        src: &Source,
        cipher: &LocationCipher,
    ) -> Decision {
        let ev = match contract::validate(key, value) {
            Ok(ev) => ev,
            Err(rej) => {
                let mut m = Map::new();
                m.insert("error_code".into(), Value::String(rej.code.as_str().into()));
                m.insert(
                    "vin".into(),
                    rej.vin.map(Value::String).unwrap_or(Value::Null),
                );
                m.insert(
                    "event_id".into(),
                    rej.event_id
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                );
                let mut s = Map::new();
                s.insert("topic".into(), Value::String(src.topic.into()));
                s.insert("partition".into(), Value::from(src.partition));
                s.insert("offset".into(), Value::from(src.offset));
                m.insert("source".into(), Value::Object(s));
                m.insert(
                    "original_payload".into(),
                    Value::String(redact::redact_payload(value)),
                );
                return Decision::Quarantine {
                    key: key.map(|k| k.to_vec()),
                    record: Value::Object(m),
                    code: rej.code,
                    event_id: rej.event_id,
                };
            }
        };
        let prev = self.state(&ev.vin).cloned();
        if let Some(p) = &prev {
            if p.recent_event_ids.iter().any(|e| e == &ev.event_id) {
                return Decision::Duplicate {
                    event_id: ev.event_id,
                };
            }
        }
        let out_of_order = prev.as_ref().is_some_and(|p| ev.ts_ms < p.max_ts_ms);
        let mut next = prev.unwrap_or_default();
        next.max_ts_ms = next.max_ts_ms.max(ev.ts_ms);
        next.recent_event_ids.push_back(ev.event_id.clone());
        while next.recent_event_ids.len() > DEDUPE_WINDOW {
            next.recent_event_ids.pop_front();
        }
        let enc = ev.location.as_ref().map(|loc| {
            serde_json::to_value(
                encrypt_location(cipher, &ev.vin, &ev.event_id, loc)
                    .expect("AES-GCM encryption with a valid key cannot fail"),
            )
            .expect("LocationEnc serializes")
        });
        let record = contract::normalize(&ev, out_of_order, enc);
        self.dirty.insert(ev.vin.clone(), (src.partition, next));
        Decision::Emit {
            key: ev.vin,
            record,
            out_of_order,
        }
    }

    /// States changed in the open transaction: (vin, input partition, state).
    pub fn dirty(&self) -> impl Iterator<Item = (&String, i32, &VinState)> {
        self.dirty.iter().map(|(k, (p, s))| (k, *p, s))
    }

    pub fn mark_committed(&mut self) {
        for (k, (_, s)) in self.dirty.drain() {
            self.committed.insert(k, s);
        }
    }

    pub fn abort(&mut self) {
        self.dirty.clear();
    }

    pub fn clear(&mut self) {
        self.dirty.clear();
        self.committed.clear();
    }

    pub fn restore(&mut self, vin: String, state: VinState) {
        self.committed.insert(vin, state);
    }

    pub fn vin_count(&self) -> usize {
        self.committed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cipher() -> LocationCipher {
        LocationCipher::from_bytes(&[7u8; 32], "t").unwrap()
    }

    fn ev(id: &str, ts: i64) -> Vec<u8> {
        format!(r#"{{"vin":"SYN-VIN-0001","event_id":"{id}","ts_ms":{ts}}}"#).into_bytes()
    }

    fn src(o: i64) -> Source<'static> {
        Source {
            topic: "t",
            partition: 0,
            offset: o,
        }
    }

    const K: Option<&[u8]> = Some(b"SYN-VIN-0001");

    #[test]
    fn dedupe_and_out_of_order() {
        let c = cipher();
        let mut p = Processor::new();
        assert!(matches!(
            p.process(K, &ev("a", 1000), &src(0), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
        assert!(matches!(
            p.process(K, &ev("b", 2000), &src(1), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
        assert!(matches!(
            p.process(K, &ev("c", 1500), &src(2), &c),
            Decision::Emit {
                out_of_order: true,
                ..
            }
        ));
        assert!(
            matches!(
                p.process(K, &ev("d", 2000), &src(3), &c),
                Decision::Emit {
                    out_of_order: false,
                    ..
                }
            ),
            "equal ts is in order"
        );
        assert!(matches!(
            p.process(K, &ev("a", 9999), &src(4), &c),
            Decision::Duplicate { .. }
        ));
        // out-of-order event did not lower the max
        assert!(matches!(
            p.process(K, &ev("e", 1999), &src(5), &c),
            Decision::Emit {
                out_of_order: true,
                ..
            }
        ));
    }

    #[test]
    fn first_event_at_ts_zero_is_in_order() {
        let c = cipher();
        let mut p = Processor::new();
        assert!(matches!(
            p.process(K, &ev("a", 0), &src(0), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
        assert!(matches!(
            p.process(K, &ev("b", 0), &src(1), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
    }

    #[test]
    fn invalid_records_do_not_touch_state() {
        let c = cipher();
        let mut p = Processor::new();
        let bad = br#"{"vin":"SYN-VIN-0001","event_id":"a","ts_ms":5000,"soc_pct":101}"#;
        assert!(matches!(
            p.process(K, bad, &src(0), &c),
            Decision::Quarantine { .. }
        ));
        assert!(
            matches!(p.process(K, bad, &src(1), &c), Decision::Quarantine { .. }),
            "re-quarantined"
        );
        assert!(matches!(
            p.process(K, &ev("a", 1000), &src(2), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
    }

    #[test]
    fn dedupe_window_is_bounded() {
        let c = cipher();
        let mut p = Processor::new();
        for i in 0..=DEDUPE_WINDOW {
            p.process(K, &ev(&format!("e{i}"), i as i64 + 1), &src(i as i64), &c);
        }
        // e0 fell out of the 1000-entry window; e1 is still inside it
        assert!(matches!(
            p.process(K, &ev("e0", 99_999), &src(5000), &c),
            Decision::Emit { .. }
        ));
        assert!(matches!(
            p.process(K, &ev("e2", 99_999), &src(5001), &c),
            Decision::Duplicate { .. }
        ));
    }

    #[test]
    fn abort_discards_uncommitted_state() {
        let c = cipher();
        let mut p = Processor::new();
        p.process(K, &ev("a", 1000), &src(0), &c);
        p.mark_committed();
        p.process(K, &ev("b", 5000), &src(1), &c);
        assert_eq!(p.dirty().count(), 1);
        p.abort();
        // replay after abort behaves exactly like the first attempt
        assert!(matches!(
            p.process(K, &ev("b", 5000), &src(1), &c),
            Decision::Emit {
                out_of_order: false,
                ..
            }
        ));
        assert!(matches!(
            p.process(K, &ev("a", 1000), &src(0), &c),
            Decision::Duplicate { .. }
        ));
    }

    #[test]
    fn restored_state_is_used() {
        let c = cipher();
        let mut p = Processor::new();
        p.restore(
            "SYN-VIN-0001".into(),
            VinState {
                max_ts_ms: 10_000,
                recent_event_ids: VecDeque::from(vec!["x".to_string()]),
            },
        );
        assert!(matches!(
            p.process(K, &ev("x", 1), &src(0), &c),
            Decision::Duplicate { .. }
        ));
        assert!(matches!(
            p.process(K, &ev("y", 9_000), &src(1), &c),
            Decision::Emit {
                out_of_order: true,
                ..
            }
        ));
    }

    #[test]
    fn quarantine_record_shape_and_no_plaintext() {
        let c = cipher();
        let mut p = Processor::new();
        let raw = br#"{"vin":"SYN-VIN-0001","event_id":"q","ts_ms":1,"soc_pct":101,"location":{"lat":10.12345,"lon":-29.12345}}"#;
        match p.process(
            K,
            raw,
            &Source {
                topic: "telemetry.battery.raw",
                partition: 3,
                offset: 77,
            },
            &c,
        ) {
            Decision::Quarantine { record, code, .. } => {
                assert_eq!(code, ErrorCode::OutOfRangeSoc);
                let s = record.to_string();
                assert!(!s.contains("10.12345") && !s.contains("29.12345"));
                assert_eq!(record["source"]["offset"], 77);
                assert_eq!(record["source"]["partition"], 3);
                assert!(record["original_payload"]
                    .as_str()
                    .unwrap()
                    .contains("[REDACTED]"));
            }
            d => panic!("{d:?}"),
        }
    }

    #[test]
    fn emitted_record_contains_no_plaintext_location() {
        let c = cipher();
        let mut p = Processor::new();
        let raw = br#"{"vin":"SYN-VIN-0001","event_id":"l","ts_ms":1,"location":{"lat":10.12345,"lon":-29.12345}}"#;
        match p.process(K, raw, &src(0), &c) {
            Decision::Emit { record, .. } => {
                let s = record.to_string();
                assert!(
                    !s.contains("10.12345")
                        && !s.contains("\"lat\"")
                        && !s.contains("\"location\"")
                );
                assert!(record["location_enc"]["ciphertext"].is_string());
            }
            d => panic!("{d:?}"),
        }
    }
}
