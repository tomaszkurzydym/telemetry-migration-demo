//! Prometheus metrics + a minimal /metrics HTTP endpoint (std only).

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

pub struct Metrics {
    pub registry: Registry,
    pub consumed: IntCounter,
    pub rows_valid: IntCounter,
    pub rows_changed: IntCounter,
    pub quarantined: IntCounterVec,
    pub db_txn_committed: IntCounter,
    pub rebalances: IntCounter,
    pub batch_commit_seconds: Histogram,
    pub record_latency_seconds: Histogram,
    pub consumer_lag: IntGaugeVec,
    pub assigned_partitions: IntGauge,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let r = Registry::new();
        let c = |n: &str, h: &str| {
            let m = IntCounter::new(n, h).unwrap();
            r.register(Box::new(m.clone())).unwrap();
            m
        };
        let consumed = c(
            "maintenance_events_consumed_total",
            "Input records consumed",
        );
        let rows_valid = c(
            "maintenance_rows_valid_total",
            "Valid records applied to the shadow table (committed)",
        );
        let rows_changed = c(
            "maintenance_rows_changed_total",
            "Upserts that inserted or changed a row (replays and late events excluded)",
        );
        let db_txn_committed = c(
            "maintenance_db_transactions_committed_total",
            "DB transactions (rows + offsets) committed",
        );
        let rebalances = c(
            "maintenance_rebalances_total",
            "Consumer group rebalance events",
        );
        let quarantined = IntCounterVec::new(
            Opts::new(
                "maintenance_events_quarantined_total",
                "Records quarantined",
            ),
            &["error_code"],
        )
        .unwrap();
        r.register(Box::new(quarantined.clone())).unwrap();
        let buckets = vec![
            0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            60.0,
        ];
        let h = |n: &str, help: &str| {
            let m =
                Histogram::with_opts(HistogramOpts::new(n, help).buckets(buckets.clone())).unwrap();
            r.register(Box::new(m.clone())).unwrap();
            m
        };
        let batch_commit_seconds = h(
            "maintenance_batch_commit_seconds",
            "Quarantine flush + DB transaction duration",
        );
        let record_latency_seconds = h(
            "maintenance_record_latency_seconds",
            "Kafka record timestamp to committed DB transaction",
        );
        let consumer_lag = IntGaugeVec::new(
            Opts::new(
                "maintenance_consumer_lag",
                "Input high watermark minus stored next offset",
            ),
            &["partition"],
        )
        .unwrap();
        r.register(Box::new(consumer_lag.clone())).unwrap();
        let assigned_partitions = IntGauge::new(
            "maintenance_assigned_partitions",
            "Currently assigned input partitions",
        )
        .unwrap();
        r.register(Box::new(assigned_partitions.clone())).unwrap();
        Arc::new(Self {
            registry: r,
            consumed,
            rows_valid,
            rows_changed,
            quarantined,
            db_txn_committed,
            rebalances,
            batch_commit_seconds,
            record_latency_seconds,
            consumer_lag,
            assigned_partitions,
        })
    }

    pub fn render(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        buf
    }
}

pub fn serve(addr: &str, metrics: Arc<Metrics>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    std::thread::Builder::new().name("metrics".into()).spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut s = stream;
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut req = [0u8; 1024];
            let _ = s.read(&mut req);
            let body = metrics.render();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = s.write_all(head.as_bytes()).and_then(|_| s.write_all(&body));
        }
    })?;
    Ok(())
}
