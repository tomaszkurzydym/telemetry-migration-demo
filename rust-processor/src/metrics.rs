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
    pub normalized: IntCounter,
    pub quarantined: IntCounterVec,
    pub deduplicated: IntCounter,
    pub out_of_order: IntCounter,
    pub location_encrypted: IntCounter,
    pub txn_committed: IntCounter,
    pub txn_aborted: IntCounter,
    pub rebalances: IntCounter,
    pub e2e_latency: Histogram,
    pub commit_latency: Histogram,
    pub restore_seconds: Histogram,
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
        let consumed = c("telemetry_events_consumed_total", "Input records consumed");
        let normalized = c(
            "telemetry_events_normalized_total",
            "Records emitted to the normalized topic (committed)",
        );
        let deduplicated = c(
            "telemetry_events_deduplicated_total",
            "Valid records dropped as duplicate event_id",
        );
        let out_of_order = c(
            "telemetry_events_out_of_order_total",
            "Records flagged out_of_order",
        );
        let location_encrypted = c(
            "telemetry_location_encrypted_total",
            "Records whose location was encrypted",
        );
        let txn_committed = c(
            "telemetry_transactions_committed_total",
            "Kafka transactions committed",
        );
        let txn_aborted = c(
            "telemetry_transactions_aborted_total",
            "Kafka transactions aborted",
        );
        let rebalances = c(
            "telemetry_rebalances_total",
            "Consumer group rebalance events",
        );
        let quarantined = IntCounterVec::new(
            Opts::new("telemetry_events_quarantined_total", "Records quarantined"),
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
        let e2e_latency = h(
            "telemetry_e2e_latency_seconds",
            "Input record timestamp to committed output",
        );
        let commit_latency = h(
            "telemetry_txn_commit_seconds",
            "send_offsets_to_transaction + commit_transaction duration",
        );
        let restore_seconds = h(
            "telemetry_state_restore_seconds",
            "State changelog restore duration",
        );
        let consumer_lag = IntGaugeVec::new(
            Opts::new(
                "telemetry_consumer_lag",
                "Input high watermark minus committed offset",
            ),
            &["partition"],
        )
        .unwrap();
        r.register(Box::new(consumer_lag.clone())).unwrap();
        let assigned_partitions = IntGauge::new(
            "telemetry_assigned_partitions",
            "Currently assigned input partitions",
        )
        .unwrap();
        r.register(Box::new(assigned_partitions.clone())).unwrap();
        Arc::new(Self {
            registry: r,
            consumed,
            normalized,
            quarantined,
            deduplicated,
            out_of_order,
            location_encrypted,
            txn_committed,
            txn_aborted,
            rebalances,
            e2e_latency,
            commit_latency,
            restore_seconds,
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
