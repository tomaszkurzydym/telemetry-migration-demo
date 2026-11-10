//! SYNTHETIC DEMO: maintenance-sync shadow processor (Kafka maintenance events -> Postgres service DB).
//! Positions are stored in Postgres in the same transaction as the row upserts (contract §8).

use log::{error, info, warn};
use maintenance_sync::contract::{self, Outcome, Row};
use maintenance_sync::db::{self, ApplyError, DbConfig, OffsetMove};
use maintenance_sync::metrics::{self, Metrics};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use std::collections::{BTreeMap, HashMap};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const EX_DATAERR: u8 = 65;
const EX_CONFIG: u8 = 78;
const LEGACY_QUARANTINE: &str = "maintenance.events.quarantine";

struct Config {
    bootstrap: String,
    input: String,
    quarantine: String,
    group: String,
    table: String,
    metrics_addr: String,
    batch_max: usize,
    batch_ms: u64,
    idle_exit_secs: u64,
}

impl Config {
    fn from_env(env: &HashMap<String, String>) -> Self {
        let get = |k: &str, d: &str| env.get(k).cloned().unwrap_or_else(|| d.to_owned());
        let num = |k: &str, d: u64| env.get(k).and_then(|v| v.parse().ok()).unwrap_or(d);
        Self {
            bootstrap: get("KAFKA_BOOTSTRAP", "localhost:9092"),
            input: get("MAINTENANCE_INPUT_TOPIC", "maintenance.events.raw"),
            quarantine: get(
                "MAINTENANCE_SHADOW_QUARANTINE_TOPIC",
                "maintenance.events.quarantine.shadow",
            ),
            group: get("MAINTENANCE_SHADOW_GROUP", "maintenance-rust-shadow-cg"),
            table: get("SERVICE_DB_SHADOW_TABLE", "service_records_shadow"),
            metrics_addr: get("METRICS_ADDR", "127.0.0.1:9465"),
            batch_max: num("BATCH_MAX", 500) as usize,
            batch_ms: num("BATCH_MS", 200),
            idle_exit_secs: num("IDLE_EXIT_SECS", 0),
        }
    }

    /// Shadow guard (contract §1): never write to the legacy table or quarantine topic.
    fn check(&self) -> Result<(), String> {
        if !db::valid_table_name(&self.table) || self.table == db::LEGACY_TABLE {
            return Err(format!(
                "SERVICE_DB_SHADOW_TABLE={} is not allowed",
                self.table
            ));
        }
        if self.quarantine == LEGACY_QUARANTINE || self.quarantine == self.input {
            return Err(format!(
                "MAINTENANCE_SHADOW_QUARANTINE_TOPIC={} is not allowed",
                self.quarantine
            ));
        }
        Ok(())
    }
}

enum Event {
    /// (partition, start offset, stored offset) from one snapshot of the offsets table.
    Assigned(Vec<(i32, i64, Option<i64>)>),
    Revoked,
    DataLoss(String),
    Fatal(String),
}

/// Assigns partitions at the positions stored in Postgres, so Kafka-committed offsets are never used.
struct Ctx {
    db: DbConfig,
    group: String,
    topic: String,
    events: Mutex<Vec<Event>>,
}

impl ClientContext for Ctx {}

impl Ctx {
    fn start_offsets(
        &self,
        c: &BaseConsumer<Self>,
        tpl: &TopicPartitionList,
    ) -> Result<Vec<(i32, i64, Option<i64>)>, Event> {
        let mut client = self
            .db
            .connect()
            .map_err(|e| Event::Fatal(format!("db connect on assign: {e}")))?;
        let stored = db::load_offsets(&mut client, &self.group, &self.topic)
            .map_err(|e| Event::Fatal(format!("load offsets: {e}")))?;
        let mut out = Vec::new();
        for e in tpl.elements_for_topic(&self.topic) {
            let p = e.partition();
            let (low, _high) = c
                .fetch_watermarks(&self.topic, p, Duration::from_secs(10))
                .map_err(|e| Event::Fatal(format!("watermarks p{p}: {e}")))?;
            let start = match stored.get(&p) {
                Some(&s) if s < low => {
                    return Err(Event::DataLoss(format!(
                        "partition {p}: stored offset {s} < earliest available {low}"
                    )))
                }
                Some(&s) => s,
                None => low,
            };
            out.push((p, start, stored.get(&p).copied()));
        }
        Ok(out)
    }
}

impl ConsumerContext for Ctx {
    fn rebalance(&self, c: &BaseConsumer<Self>, err: RDKafkaRespErr, tpl: &mut TopicPartitionList) {
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                match self.start_offsets(c, tpl) {
                    Ok(starts) => {
                        let mut a = TopicPartitionList::new();
                        for &(p, o, _) in &starts {
                            let _ = a.add_partition_offset(&self.topic, p, Offset::Offset(o));
                        }
                        match c.assign(&a) {
                            Ok(()) => self.events.lock().unwrap().push(Event::Assigned(starts)),
                            Err(e) => self
                                .events
                                .lock()
                                .unwrap()
                                .push(Event::Fatal(format!("assign: {e}"))),
                        }
                    }
                    Err(ev) => {
                        let _ = c.assign(&TopicPartitionList::new());
                        self.events.lock().unwrap().push(ev);
                    }
                }
            }
            _ => {
                self.events.lock().unwrap().push(Event::Revoked);
                let _ = c.unassign();
            }
        }
    }
}

#[derive(Default)]
struct DeliveryCtx {
    failed: AtomicU64,
}
impl ClientContext for DeliveryCtx {}
impl ProducerContext for DeliveryCtx {
    type DeliveryOpaque = ();
    fn delivery(&self, r: &DeliveryResult<'_>, _: ()) {
        if r.is_err() {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

type Cons = BaseConsumer<Ctx>;
type Prod = ThreadedProducer<DeliveryCtx>;

#[derive(Default)]
struct Buffer {
    rows: Vec<Row>,
    quarantine: Vec<(Option<String>, String, &'static str)>,
    touched: BTreeMap<i32, ()>,
    kafka_ts_ms: Vec<i64>,
    since: Option<Instant>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    let env: HashMap<String, String> = std::env::vars().collect();
    // Hard-fail before touching Kafka or the DB (contract §8).
    let db_cfg = match DbConfig::from_env(&env) {
        Ok(c) => c,
        Err(e) => {
            error!("FATAL startup: {e}; refusing to start");
            return ExitCode::from(EX_CONFIG);
        }
    };
    let cfg = Config::from_env(&env);
    if let Err(e) = cfg.check() {
        error!("FATAL startup: {e}; refusing to start");
        return ExitCode::from(EX_CONFIG);
    }
    match run(cfg, db_cfg) {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => ExitCode::from(code),
    }
}

fn run(cfg: Config, db_cfg: DbConfig) -> Result<(), u8> {
    let fatal = |what: &str, e: &dyn std::fmt::Display| {
        error!("FATAL {what}: {e}");
        2u8
    };
    let mut client = db_cfg.connect().map_err(|e| fatal("db connect", &e))?;
    for t in [cfg.table.as_str(), "maintenance_shadow_offsets"] {
        let row = client
            .query_one("SELECT to_regclass($1)::text", &[&t])
            .map_err(|e| fatal("db check", &e))?;
        if row.get::<_, Option<String>>(0).is_none() {
            error!("FATAL startup: table {t} missing; apply sql/shadow_schema.sql");
            return Err(EX_CONFIG);
        }
    }
    let m = Metrics::new();
    metrics::serve(&cfg.metrics_addr, m.clone()).map_err(|e| fatal("metrics", &e))?;
    let term = Arc::new(AtomicBool::new(false));
    for s in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(s, term.clone()).map_err(|e| fatal("signal", &e))?;
    }
    info!(
        "starting group={} input={} table={} quarantine={} db={:?}",
        cfg.group, cfg.input, cfg.table, cfg.quarantine, db_cfg
    );
    let consumer: Cons = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", &cfg.group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "error")
        .set("partition.assignment.strategy", "range")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .create_with_context(Ctx {
            db: db_cfg.clone(),
            group: cfg.group.clone(),
            topic: cfg.input.clone(),
            events: Mutex::new(Vec::new()),
        })
        .map_err(|e| fatal("consumer", &e))?;
    let producer: Prod = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "5")
        .create_with_context(DeliveryCtx::default())
        .map_err(|e| fatal("producer", &e))?;
    consumer
        .subscribe(&[&cfg.input])
        .map_err(|e| fatal("subscribe", &e))?;

    let mut next: HashMap<i32, i64> = HashMap::new(); // next offset to process, per owned partition
    let mut stored: HashMap<i32, Option<i64>> = HashMap::new(); // position as last read/written in the DB
    let mut buf = Buffer::default();
    let mut last_msg = Instant::now();
    let mut last_lag = Instant::now();
    let positions: Positions = Arc::new(Mutex::new(HashMap::new()));
    let lag_total = Arc::new(AtomicI64::new(-1));
    spawn_lag_monitor(&cfg, positions.clone(), lag_total.clone(), m.clone())
        .map_err(|e| fatal("lag monitor", &e))?;
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();

    loop {
        let stopping = term.load(Ordering::Relaxed);
        let polled = if stopping {
            None
        } else {
            consumer.poll(Duration::from_millis(50))
        };
        let events = std::mem::take(&mut *consumer.context().events.lock().unwrap());
        for ev in events {
            match ev {
                Event::Revoked => {
                    m.rebalances.inc();
                    if !buf.rows.is_empty() || !buf.quarantine.is_empty() {
                        info!(
                            "partitions revoked: dropping {} uncommitted records",
                            buf.rows.len() + buf.quarantine.len()
                        );
                    }
                    buf = Buffer::default();
                    next.clear();
                    stored.clear();
                    m.assigned_partitions.set(0);
                }
                Event::Assigned(starts) => {
                    m.rebalances.inc();
                    // The CAS expectation must be the snapshot the start offsets came from: if another writer
                    // committed since, the next transaction is fenced instead of moving its position backwards.
                    for &(p, o, s) in &starts {
                        next.insert(p, o);
                        stored.insert(p, s);
                    }
                    info!("assigned partitions at stored positions: {starts:?}");
                    m.assigned_partitions.set(next.len() as i64);
                }
                Event::DataLoss(e) => {
                    error!("FATAL data loss: {e}; refusing to continue (contract §8)");
                    return Err(EX_DATAERR);
                }
                Event::Fatal(e) => return Err(fatal("rebalance", &e)),
            }
        }
        match polled {
            Some(Ok(bm)) => {
                let msg: OwnedMessage = bm.detach();
                let p = msg.partition();
                let off = msg.offset();
                if let Some(n) = next.get_mut(&p) {
                    if off >= *n {
                        *n = off + 1;
                        last_msg = Instant::now();
                        m.consumed.inc();
                        let key = msg.key().and_then(|k| std::str::from_utf8(k).ok());
                        match contract::validate(key, msg.payload()) {
                            Outcome::Valid(r) => buf.rows.push(r),
                            Outcome::Rejected(r) => {
                                info!("{}", contract::log_line(&r));
                                let q = contract::quarantine(&r, msg.payload(), &cfg.input, p, off);
                                buf.quarantine.push((r.vin.clone(), q, r.code));
                            }
                        }
                        buf.touched.insert(p, ());
                        if let Some(ts) = msg.timestamp().to_millis() {
                            buf.kafka_ts_ms.push(ts);
                        }
                        buf.since.get_or_insert_with(Instant::now);
                    }
                }
            }
            Some(Err(KafkaError::MessageConsumption(
                code @ (RDKafkaErrorCode::OffsetOutOfRange | RDKafkaErrorCode::AutoOffsetReset),
            ))) => {
                error!("FATAL data loss: consumer reported {code:?}; refusing to continue (contract §8)");
                return Err(EX_DATAERR);
            }
            Some(Err(e)) => warn!("consumer error: {e}"),
            None => {}
        }

        let due = buf
            .since
            .is_some_and(|t| t.elapsed() >= Duration::from_millis(cfg.batch_ms));
        if buf.rows.len() + buf.quarantine.len() >= cfg.batch_max
            || due
            || (stopping && buf.since.is_some())
        {
            let b = std::mem::take(&mut buf);
            flush(
                &cfg,
                &mut client,
                &producer,
                &m,
                &next,
                &mut stored,
                b,
                &mut totals,
            )?;
        }

        if last_lag.elapsed() >= Duration::from_secs(1) {
            last_lag = Instant::now();
            *positions.lock().unwrap() = next
                .iter()
                .map(|(&p, &n)| (p, stored.get(&p).copied().flatten().unwrap_or(n)))
                .collect();
            let idle = cfg.idle_exit_secs > 0
                && last_msg.elapsed() >= Duration::from_secs(cfg.idle_exit_secs);
            if idle && lag_total.load(Ordering::Relaxed) == 0 && buf.since.is_none() {
                info!("idle with zero lag for {}s: exiting", cfg.idle_exit_secs);
                break;
            }
        }
        if stopping && buf.since.is_none() {
            break;
        }
    }
    info!("summary {}", serde_json::to_string(&totals).unwrap());
    Ok(())
}

/// Stored (or start) position per owned partition, published by the processing loop.
type Positions = Arc<Mutex<HashMap<i32, i64>>>;

/// Lag = high watermark - stored position, measured on a separate client so the blocking
/// ListOffsets round-trips never stall the processing loop's fetches.
fn spawn_lag_monitor(
    cfg: &Config,
    positions: Positions,
    total: Arc<AtomicI64>,
    m: Arc<Metrics>,
) -> Result<(), KafkaError> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .set("group.id", format!("{}-lag-monitor", cfg.group))
        .create()?;
    let topic = cfg.input.clone();
    std::thread::spawn(move || loop {
        let snapshot: Vec<(i32, i64)> = positions
            .lock()
            .unwrap()
            .iter()
            .map(|(&p, &o)| (p, o))
            .collect();
        let mut sum = 0;
        let mut ok = !snapshot.is_empty();
        for (p, off) in snapshot {
            match c.fetch_watermarks(&topic, p, Duration::from_secs(5)) {
                Ok((_, hi)) => {
                    let lag = (hi - off).max(0);
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

#[allow(clippy::too_many_arguments)]
fn flush(
    cfg: &Config,
    client: &mut postgres::Client,
    producer: &Prod,
    m: &Metrics,
    next: &HashMap<i32, i64>,
    stored: &mut HashMap<i32, Option<i64>>,
    b: Buffer,
    totals: &mut BTreeMap<&'static str, u64>,
) -> Result<(), u8> {
    let t0 = Instant::now();
    let failed_before = producer.context().failed.load(Ordering::Relaxed);
    for (key, json, _) in &b.quarantine {
        let mut rec = BaseRecord::<str, str>::to(&cfg.quarantine).payload(json.as_str());
        if let Some(k) = key {
            rec = rec.key(k.as_str());
        }
        let mut rec = rec;
        loop {
            match producer.send(rec) {
                Ok(()) => break,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    rec = r;
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err((e, _)) => {
                    error!("FATAL quarantine produce: {e}");
                    return Err(2);
                }
            }
        }
    }
    if let Err(e) = producer.flush(Duration::from_secs(30)) {
        error!("FATAL quarantine flush: {e}");
        return Err(2);
    }
    if producer.context().failed.load(Ordering::Relaxed) != failed_before {
        error!("FATAL quarantine delivery failed; DB transaction not committed");
        return Err(2);
    }
    let moves: Vec<OffsetMove> = b
        .touched
        .keys()
        .map(|&p| OffsetMove {
            partition: p,
            prev: stored.get(&p).copied().flatten(),
            next: next[&p],
        })
        .collect();
    match db::apply_batch(client, &cfg.table, &cfg.group, &cfg.input, &moves, &b.rows) {
        Ok(changed) => {
            for mv in &moves {
                stored.insert(mv.partition, Some(mv.next));
            }
            m.db_txn_committed.inc();
            m.rows_valid.inc_by(b.rows.len() as u64);
            m.rows_changed.inc_by(changed);
            for (_, _, code) in &b.quarantine {
                m.quarantined.with_label_values(&[code]).inc();
            }
            let now = now_ms();
            for ts in &b.kafka_ts_ms {
                m.record_latency_seconds
                    .observe((now - ts).max(0) as f64 / 1000.0);
            }
            m.batch_commit_seconds.observe(t0.elapsed().as_secs_f64());
            *totals.entry("consumed").or_default() += (b.rows.len() + b.quarantine.len()) as u64;
            *totals.entry("rows_valid").or_default() += b.rows.len() as u64;
            *totals.entry("rows_changed").or_default() += changed;
            *totals.entry("quarantined").or_default() += b.quarantine.len() as u64;
            *totals.entry("db_transactions").or_default() += 1;
            Ok(())
        }
        Err(ApplyError::Fenced(p)) => {
            error!(
                "FATAL fenced: stored position for partition {p} moved; another instance owns it"
            );
            Err(3)
        }
        Err(e) => Err(fatal_db(&e)),
    }
}

fn fatal_db(e: &ApplyError) -> u8 {
    error!("FATAL {e}");
    2
}
