//! SYNTHETIC DEMO — Rust Kafka stream processor (shadow mode by default).
//!
//! Consume-transform-produce with Kafka transactions: each batch's outputs, quarantine records,
//! per-VIN state changelog records and consumer offsets are committed atomically
//! (contract §8). Offsets are never auto-committed.

use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    BaseConsumer, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
};
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, Timestamp};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use telemetry_processor::crypto::LocationCipher;
use telemetry_processor::metrics::{self, Metrics};
use telemetry_processor::pipeline::{Decision, Processor, Source, VinState};
use telemetry_processor::redact::sanitize_event_id;

const TXN_TIMEOUT: Duration = Duration::from_secs(30);

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| d.to_string())
}

fn env_u64(k: &str, d: u64) -> u64 {
    env(k, &d.to_string()).parse().unwrap_or(d)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Config {
    bootstrap: String,
    input: String,
    output: String,
    quarantine: String,
    state: String,
    group: String,
    instance: String,
    metrics_addr: String,
    batch_max: usize,
    batch_ms: u64,
    exit_when_idle_secs: u64,
    summary_file: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            bootstrap: env("KAFKA_BOOTSTRAP", "localhost:9092"),
            input: env("INPUT_TOPIC", "telemetry.battery.raw"),
            output: env("OUTPUT_TOPIC", "telemetry.battery.normalized.shadow"),
            quarantine: env("QUARANTINE_TOPIC", "telemetry.battery.quarantine.shadow"),
            state: env("STATE_TOPIC", "telemetry.battery.rust-shadow.state"),
            group: env("CONSUMER_GROUP", "rust-shadow-cg"),
            instance: env("PROCESSOR_INSTANCE_ID", "0"),
            metrics_addr: env("METRICS_ADDR", "0.0.0.0:9100"),
            batch_max: env_u64("BATCH_MAX", 500) as usize,
            batch_ms: env_u64("BATCH_MAX_MS", 100),
            exit_when_idle_secs: env_u64("EXIT_WHEN_IDLE_SECS", 0),
            summary_file: std::env::var("SUMMARY_FILE").ok().filter(|v| !v.is_empty()),
        }
    }
}

enum RebalanceEvent {
    Revoke,
    Assign(Vec<i32>),
}

#[derive(Default)]
struct Ctx {
    events: Mutex<Vec<RebalanceEvent>>,
}

impl ClientContext for Ctx {}

impl ConsumerContext for Ctx {
    fn pre_rebalance(&self, _c: &BaseConsumer<Self>, r: &Rebalance<'_>) {
        if let Rebalance::Revoke(_) = r {
            self.events.lock().unwrap().push(RebalanceEvent::Revoke);
        }
    }

    fn post_rebalance(&self, _c: &BaseConsumer<Self>, r: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = r {
            let parts = tpl.elements().iter().map(|e| e.partition()).collect();
            self.events
                .lock()
                .unwrap()
                .push(RebalanceEvent::Assign(parts));
        }
    }
}

type Cons = BaseConsumer<Ctx>;
type Prod = ThreadedProducer<DefaultProducerContext>;

struct Batch {
    open: bool,
    started: Instant,
    count: usize,
    offsets: BTreeMap<i32, i64>,
    input_ts_ms: Vec<i64>,
    normalized: u64,
    out_of_order: u64,
    encrypted: u64,
    quarantined: Vec<&'static str>,
    deduplicated: u64,
}

impl Batch {
    fn new() -> Self {
        Self {
            open: false,
            started: Instant::now(),
            count: 0,
            offsets: BTreeMap::new(),
            input_ts_ms: Vec::new(),
            normalized: 0,
            out_of_order: 0,
            encrypted: 0,
            quarantined: Vec::new(),
            deduplicated: 0,
        }
    }
}

fn send(p: &Prod, rec: BaseRecord<'_, [u8], [u8]>) -> Result<(), KafkaError> {
    let mut rec = rec;
    loop {
        match p.send(rec) {
            Ok(()) => return Ok(()),
            Err((
                KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull),
                r,
            )) => {
                rec = r;
                std::thread::sleep(Duration::from_millis(5));
            }
            Err((e, _)) => return Err(e),
        }
    }
}

fn restore_state(
    cfg: &Config,
    parts: &[i32],
    proc_: &mut Processor,
    m: &Metrics,
) -> Result<(), KafkaError> {
    let t0 = Instant::now();
    let c: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", format!("{}-restore", cfg.group))
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true")
        .create()?;
    let mut tpl = TopicPartitionList::new();
    let mut pending = BTreeSet::new();
    for &p in parts {
        let (lo, hi) = c.fetch_watermarks(&cfg.state, p, Duration::from_secs(10))?;
        if hi > lo {
            tpl.add_partition_offset(&cfg.state, p, Offset::Beginning)?;
            pending.insert(p);
        }
    }
    let mut restored = 0usize;
    if !pending.is_empty() {
        c.assign(&tpl)?;
        let deadline = Instant::now() + Duration::from_secs(120);
        while !pending.is_empty() && Instant::now() < deadline {
            match c.poll(Duration::from_millis(200)) {
                Some(Ok(msg)) => {
                    if let (Some(k), Some(v)) = (msg.key(), msg.payload()) {
                        if let Ok(st) = serde_json::from_slice::<VinState>(v) {
                            proc_.restore(String::from_utf8_lossy(k).into_owned(), st);
                            restored += 1;
                        }
                    }
                }
                Some(Err(KafkaError::PartitionEOF(p))) => {
                    pending.remove(&p);
                }
                Some(Err(e)) => warn!("state restore error_code={e}"),
                None => {}
            }
        }
        if !pending.is_empty() {
            return Err(KafkaError::Canceled);
        }
    }
    m.restore_seconds.observe(t0.elapsed().as_secs_f64());
    info!(
        "state restored partitions={parts:?} records={restored} vins={} in {:?}",
        proc_.vin_count(),
        t0.elapsed()
    );
    Ok(())
}

fn rewind_to_committed(c: &Cons) -> Result<(), KafkaError> {
    let committed = c.committed(Duration::from_secs(10))?;
    for e in committed.elements() {
        let off = match e.offset() {
            Offset::Offset(o) => Offset::Offset(o),
            _ => Offset::Beginning,
        };
        c.seek(e.topic(), e.partition(), off, Duration::from_secs(10))?;
    }
    Ok(())
}

/// Committed next-offset per assigned input partition (-1 = no committed offset yet).
type Committed = Arc<Mutex<BTreeMap<i32, i64>>>;

/// Lag = high watermark - committed offset, measured on a separate client so the
/// blocking ListOffsets round-trips never stall the processing loop.
fn spawn_lag_monitor(
    cfg: &Config,
    committed: Committed,
    total: Arc<AtomicI64>,
    m: Arc<Metrics>,
) -> Result<(), KafkaError> {
    let c: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", format!("{}-lag-monitor", cfg.group))
        .create()?;
    let topic = cfg.input.clone();
    std::thread::spawn(move || loop {
        let snapshot: Vec<(i32, i64)> = committed
            .lock()
            .unwrap()
            .iter()
            .map(|(p, o)| (*p, *o))
            .collect();
        let mut sum = 0i64;
        let mut ok = !snapshot.is_empty();
        for (p, off) in snapshot {
            match c.fetch_watermarks(&topic, p, Duration::from_secs(5)) {
                Ok((lo, hi)) => {
                    let lag = (hi - if off < 0 { lo } else { off }).max(0);
                    m.consumer_lag.with_label_values(&[&p.to_string()]).set(lag);
                    sum += lag;
                }
                Err(_) => ok = false,
            }
        }
        total.store(if ok { sum } else { -1 }, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(500));
    });
    Ok(())
}

fn seed_committed(
    c: &Cons,
    input: &str,
    parts: &[i32],
    committed: &Committed,
) -> Result<(), KafkaError> {
    let tpl = c.committed(Duration::from_secs(10))?;
    let mut map = committed.lock().unwrap();
    map.clear();
    for &p in parts {
        let off = match tpl.find_partition(input, p).map(|e| e.offset()) {
            Some(Offset::Offset(o)) => o,
            _ => -1,
        };
        map.insert(p, off);
    }
    Ok(())
}

fn percentile(sorted: &[i64], q: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx]
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    // Hard-fail before touching Kafka (contract §5).
    let cipher = match LocationCipher::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!("FATAL startup: {e}; refusing to start");
            return ExitCode::from(78);
        }
    };
    let cfg = Config::from_env();
    match run(cfg, cipher) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("FATAL: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(cfg: Config, cipher: LocationCipher) -> Result<(), Box<dyn std::error::Error>> {
    let m = Metrics::new();
    metrics::serve(&cfg.metrics_addr, m.clone())?;
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, term.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, term.clone())?;

    let txn_id = format!("{}-{}", cfg.group, cfg.instance);
    info!(
        "starting instance={} txn_id={txn_id} group={} input={} output={} quarantine={} key_id={}",
        cfg.instance,
        cfg.group,
        cfg.input,
        cfg.output,
        cfg.quarantine,
        cipher.key_id()
    );
    let consumer: Cons = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", &cfg.group)
        .set("client.id", &txn_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("partition.assignment.strategy", "range")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .set("max.poll.interval.ms", "60000")
        .create_with_context(Ctx::default())?;
    let producer: Prod = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("transactional.id", &txn_id)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("partitioner", "murmur2_random")
        .set("transaction.timeout.ms", "10000")
        .set("linger.ms", "5")
        // librdkafka retries CONCURRENT_TRANSACTIONS (broker still writing the previous txn's
        // markers) with exponential backoff up to retry.backoff.max.ms (default 1s).
        .set("retry.backoff.ms", "10")
        .set("retry.backoff.max.ms", "50")
        .create()?;
    producer.init_transactions(TXN_TIMEOUT)?;
    consumer.subscribe(&[&cfg.input])?;

    let mut proc_ = Processor::new();
    let mut batch = Batch::new();
    let mut assigned: Vec<i32> = Vec::new();
    let mut last_msg = Instant::now();
    let committed: Committed = Arc::new(Mutex::new(BTreeMap::new()));
    let total_lag = Arc::new(AtomicI64::new(-1));
    spawn_lag_monitor(&cfg, committed.clone(), total_lag.clone(), m.clone())?;
    let mut first_msg_ms: Option<i64> = None;
    let mut last_commit_ms: i64 = 0;
    let mut all_latencies: Vec<i64> = Vec::new();
    let mut totals = BTreeMap::<&str, u64>::new();

    let abort = |producer: &Prod, proc_: &mut Processor, batch: &mut Batch, m: &Metrics| {
        if batch.open {
            if let Err(e) = producer.abort_transaction(TXN_TIMEOUT) {
                warn!("abort_transaction error_code={e}");
            }
            m.txn_aborted.inc();
        }
        proc_.abort();
        *batch = Batch::new();
    };

    loop {
        let stopping = term.load(Ordering::Relaxed);
        let polled = if stopping {
            None
        } else {
            consumer.poll(Duration::from_millis(20))
        };

        let events: Vec<RebalanceEvent> =
            std::mem::take(&mut *consumer.context().events.lock().unwrap());
        for ev in events {
            m.rebalances.inc();
            match ev {
                RebalanceEvent::Revoke => {
                    if batch.open {
                        info!(
                            "partitions revoked: aborting open transaction ({} records)",
                            batch.count
                        );
                    }
                    abort(&producer, &mut proc_, &mut batch, &m);
                    proc_.clear();
                    assigned.clear();
                    committed.lock().unwrap().clear();
                    total_lag.store(-1, Ordering::Relaxed);
                }
                RebalanceEvent::Assign(parts) => {
                    proc_.clear();
                    restore_state(&cfg, &parts, &mut proc_, &m)?;
                    seed_committed(&consumer, &cfg.input, &parts, &committed)?;
                    total_lag.store(-1, Ordering::Relaxed);
                    assigned = parts;
                }
            }
            m.assigned_partitions.set(assigned.len() as i64);
        }

        let got_msg = matches!(polled, Some(Ok(_)));
        match polled {
            None => {}
            Some(Err(e)) => warn!("consumer error_code={e}"),
            Some(Ok(msg)) => {
                if !batch.open {
                    producer.begin_transaction()?;
                    batch.open = true;
                    batch.started = Instant::now();
                }
                last_msg = Instant::now();
                first_msg_ms.get_or_insert_with(now_ms);
                let src = Source {
                    topic: msg.topic(),
                    partition: msg.partition(),
                    offset: msg.offset(),
                };
                let value = msg.payload().unwrap_or(&[]);
                match proc_.process(msg.key(), value, &src, &cipher) {
                    Decision::Emit {
                        key,
                        record,
                        out_of_order,
                    } => {
                        let payload = serde_json::to_vec(&record)?;
                        if record.get("location_enc").is_some() {
                            batch.encrypted += 1;
                        }
                        send(
                            &producer,
                            BaseRecord::to(&cfg.output)
                                .key(key.as_bytes())
                                .payload(&payload[..]),
                        )?;
                        batch.normalized += 1;
                        batch.out_of_order += out_of_order as u64;
                    }
                    Decision::Quarantine {
                        key,
                        record,
                        code,
                        event_id,
                    } => {
                        warn!(
                            "quarantined event_id={} error_code={}",
                            sanitize_event_id(event_id.as_deref()),
                            code.as_str()
                        );
                        let payload = serde_json::to_vec(&record)?;
                        let mut rec = BaseRecord::to(&cfg.quarantine).payload(&payload[..]);
                        if let Some(k) = key.as_deref() {
                            rec = rec.key(k);
                        }
                        send(&producer, rec)?;
                        batch.quarantined.push(code.as_str());
                    }
                    Decision::Duplicate { .. } => batch.deduplicated += 1,
                }
                if let Timestamp::CreateTime(t) | Timestamp::LogAppendTime(t) = msg.timestamp() {
                    batch.input_ts_ms.push(t);
                }
                batch.offsets.insert(msg.partition(), msg.offset() + 1);
                batch.count += 1;
            }
        }

        let due = batch.open
            && (stopping
                || batch.count >= cfg.batch_max
                || batch.started.elapsed() >= Duration::from_millis(cfg.batch_ms)
                || !got_msg);
        if due {
            match commit_batch(&cfg, &producer, &consumer, &proc_, &batch, &m) {
                Ok(()) => {
                    proc_.mark_committed();
                    let now = now_ms();
                    last_commit_ms = now;
                    m.consumed.inc_by(batch.count as u64);
                    m.normalized.inc_by(batch.normalized);
                    m.out_of_order.inc_by(batch.out_of_order);
                    m.deduplicated.inc_by(batch.deduplicated);
                    m.location_encrypted.inc_by(batch.encrypted);
                    for c in &batch.quarantined {
                        m.quarantined.with_label_values(&[c]).inc();
                    }
                    *totals.entry("consumed").or_default() += batch.count as u64;
                    *totals.entry("normalized").or_default() += batch.normalized;
                    *totals.entry("quarantined").or_default() += batch.quarantined.len() as u64;
                    *totals.entry("deduplicated").or_default() += batch.deduplicated;
                    *totals.entry("out_of_order").or_default() += batch.out_of_order;
                    *totals.entry("location_encrypted").or_default() += batch.encrypted;
                    *totals.entry("txn_committed").or_default() += 1;
                    for t in &batch.input_ts_ms {
                        let l = (now - t).max(0);
                        m.e2e_latency.observe(l as f64 / 1000.0);
                        if all_latencies.len() < 20_000_000 {
                            all_latencies.push(l);
                        }
                    }
                    m.txn_committed.inc();
                    {
                        let mut map = committed.lock().unwrap();
                        for (p, o) in &batch.offsets {
                            map.insert(*p, *o);
                        }
                    }
                    total_lag.store(-1, Ordering::Relaxed);
                    batch = Batch::new();
                }
                Err(e) => {
                    let fatal = matches!(&e, KafkaError::Transaction(t) if t.is_fatal());
                    warn!("transaction commit failed error_code={e} fatal={fatal}");
                    if fatal {
                        return Err(Box::new(e));
                    }
                    abort(&producer, &mut proc_, &mut batch, &m);
                    *totals.entry("txn_aborted").or_default() += 1;
                    rewind_to_committed(&consumer)?;
                }
            }
        }

        if stopping && !batch.open {
            info!("SIGTERM/SIGINT: committed open work, shutting down");
            break;
        }
        if cfg.exit_when_idle_secs > 0
            && !batch.open
            && !assigned.is_empty()
            && total_lag.load(Ordering::Relaxed) == 0
            && last_msg.elapsed() >= Duration::from_secs(cfg.exit_when_idle_secs)
        {
            info!(
                "idle with zero committed lag for {}s: exiting",
                cfg.exit_when_idle_secs
            );
            break;
        }
    }

    if let Some(path) = &cfg.summary_file {
        all_latencies.sort_unstable();
        let window_ms = first_msg_ms
            .map(|f| (last_commit_ms - f).max(1))
            .unwrap_or(0);
        let consumed = *totals.get("consumed").unwrap_or(&0);
        let summary = serde_json::json!({
            "instance": cfg.instance,
            "totals": totals,
            "first_msg_unix_ms": first_msg_ms,
            "last_commit_unix_ms": last_commit_ms,
            "processing_window_ms": window_ms,
            "throughput_events_per_s": if window_ms > 0 { consumed as f64 * 1000.0 / window_ms as f64 } else { 0.0 },
            "e2e_latency_ms": {
                "samples": all_latencies.len(),
                "p50": percentile(&all_latencies, 0.50),
                "p95": percentile(&all_latencies, 0.95),
                "p99": percentile(&all_latencies, 0.99),
                "max": all_latencies.last().copied().unwrap_or(0),
            },
            "batch_max": cfg.batch_max,
            "batch_max_ms": cfg.batch_ms,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&summary)?)?;
    }
    drop(consumer);
    Ok(())
}

fn commit_batch(
    cfg: &Config,
    producer: &Prod,
    consumer: &Cons,
    proc_: &Processor,
    batch: &Batch,
    m: &Metrics,
) -> Result<(), KafkaError> {
    for (vin, partition, st) in proc_.dirty() {
        let v = serde_json::to_vec(st).expect("state serializes");
        send(
            producer,
            BaseRecord::to(&cfg.state)
                .partition(partition)
                .key(vin.as_bytes())
                .payload(&v[..]),
        )?;
    }
    let t0 = Instant::now();
    let mut tpl = TopicPartitionList::new();
    for (p, o) in &batch.offsets {
        tpl.add_partition_offset(&cfg.input, *p, Offset::Offset(*o))?;
    }
    let gm = consumer.group_metadata().ok_or(KafkaError::Canceled)?;
    producer.send_offsets_to_transaction(&tpl, &gm, TXN_TIMEOUT)?;
    producer.commit_transaction(TXN_TIMEOUT)?;
    m.commit_latency.observe(t0.elapsed().as_secs_f64());
    log::debug!(
        "txn committed records={} commit_ms={}",
        batch.count,
        t0.elapsed().as_millis()
    );
    Ok(())
}
