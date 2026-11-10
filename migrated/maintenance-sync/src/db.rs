//! Service DB access: config, shadow upsert and in-transaction offset store (contract §6, §8).

use crate::contract::{Row, SCHEMA_V};
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::fmt;

pub const WRITTEN_BY: &str = "rust-maintenance-sync";
pub const LEGACY_TABLE: &str = "service_records";

#[derive(Clone)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub user: String,
    pub password: String,
}

impl fmt::Debug for DbConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DbConfig({}:{}/{}, user={}, password=***)",
            self.host, self.port, self.name, self.user
        )
    }
}

impl DbConfig {
    pub fn from_env(env: &HashMap<String, String>) -> Result<Self, String> {
        let get = |k: &str, d: &str| env.get(k).cloned().unwrap_or_else(|| d.to_owned());
        let password = env
            .get("SERVICE_DB_PASSWORD")
            .filter(|p| !p.is_empty())
            .cloned()
            .ok_or("SERVICE_DB_PASSWORD is not set")?;
        let port = get("SERVICE_DB_PORT", "5432")
            .parse()
            .map_err(|_| "SERVICE_DB_PORT is not a port number")?;
        Ok(Self {
            host: get("SERVICE_DB_HOST", "localhost"),
            port,
            name: get("SERVICE_DB_NAME", "service_db"),
            user: get("SERVICE_DB_USER", "service"),
            password,
        })
    }

    pub fn connect(&self) -> Result<Client, postgres::Error> {
        postgres::Config::new()
            .host(&self.host)
            .port(self.port)
            .dbname(&self.name)
            .user(&self.user)
            .password(&self.password)
            .application_name(WRITTEN_BY)
            .connect(NoTls)
    }
}

pub fn valid_table_name(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= 63
        && t.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && t.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Same rule as legacy ServiceDb.UpsertSql: newer-or-equal event_time and a real change, else no-op.
pub fn upsert_sql(table: &str) -> String {
    assert!(valid_table_name(table));
    format!(
        "INSERT INTO {t} (work_order_id, vin, status, service_center_id, event_time, odometer_km, dtc_codes, \
         parts_count, labor_min, schema_v, written_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT (work_order_id) DO UPDATE SET vin = EXCLUDED.vin, status = EXCLUDED.status, \
         service_center_id = EXCLUDED.service_center_id, event_time = EXCLUDED.event_time, \
         odometer_km = EXCLUDED.odometer_km, dtc_codes = EXCLUDED.dtc_codes, parts_count = EXCLUDED.parts_count, \
         labor_min = EXCLUDED.labor_min, schema_v = EXCLUDED.schema_v, written_by = EXCLUDED.written_by, \
         updated_at = now() \
         WHERE EXCLUDED.event_time >= {t}.event_time AND \
         ({t}.vin, {t}.status, {t}.service_center_id, {t}.event_time, {t}.odometer_km, {t}.dtc_codes, \
          {t}.parts_count, {t}.labor_min, {t}.schema_v, {t}.written_by) IS DISTINCT FROM \
         (EXCLUDED.vin, EXCLUDED.status, EXCLUDED.service_center_id, EXCLUDED.event_time, EXCLUDED.odometer_km, \
          EXCLUDED.dtc_codes, EXCLUDED.parts_count, EXCLUDED.labor_min, EXCLUDED.schema_v, EXCLUDED.written_by)",
        t = table
    )
}

pub fn event_time(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).expect("ts_ms validated to contract range")
}

/// Stored next offset per partition for (group, topic).
pub fn load_offsets(
    c: &mut Client,
    group: &str,
    topic: &str,
) -> Result<HashMap<i32, i64>, postgres::Error> {
    Ok(c
        .query(
            "SELECT partition, next_offset FROM maintenance_shadow_offsets WHERE consumer_group = $1 AND topic = $2",
            &[&group, &topic],
        )?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect())
}

/// Position change for one partition: `prev` is what this writer last read from the DB.
pub struct OffsetMove {
    pub partition: i32,
    pub prev: Option<i64>,
    pub next: i64,
}

#[derive(Debug)]
pub enum ApplyError {
    /// Another writer moved the stored position: this instance no longer owns the partition.
    Fenced(i32),
    Db(postgres::Error),
}

impl From<postgres::Error> for ApplyError {
    fn from(e: postgres::Error) -> Self {
        ApplyError::Db(e)
    }
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::Fenced(p) => write!(f, "fenced on partition {p}"),
            ApplyError::Db(e) => write!(f, "db error: {e}"),
        }
    }
}

/// One transaction: move the stored positions (fenced compare-and-set), then upsert rows in input order.
/// Returns the number of rows actually inserted or changed.
pub fn apply_batch(
    c: &mut Client,
    table: &str,
    group: &str,
    topic: &str,
    moves: &[OffsetMove],
    rows: &[Row],
) -> Result<u64, ApplyError> {
    let mut tx = c.transaction()?;
    for m in moves {
        let n = match m.prev {
            Some(prev) => tx.execute(
                "UPDATE maintenance_shadow_offsets SET next_offset = $4, updated_at = now() \
                 WHERE consumer_group = $1 AND topic = $2 AND partition = $3 AND next_offset = $5",
                &[&group, &topic, &m.partition, &m.next, &prev],
            )?,
            None => tx.execute(
                "INSERT INTO maintenance_shadow_offsets (consumer_group, topic, partition, next_offset) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[&group, &topic, &m.partition, &m.next],
            )?,
        };
        if n != 1 {
            return Err(ApplyError::Fenced(m.partition));
        }
    }
    let st = tx.prepare(&upsert_sql(table))?;
    let mut changed = 0;
    for r in rows {
        changed += tx.execute(
            &st,
            &[
                &r.work_order_id,
                &r.vin,
                &r.status,
                &r.service_center_id,
                &event_time(r.event_time_ms),
                &r.odometer_km,
                &r.dtc_codes,
                &r.parts_count,
                &r.labor_min,
                &SCHEMA_V,
                &WRITTEN_BY,
            ],
        )?;
    }
    tx.commit()?;
    Ok(changed)
}
