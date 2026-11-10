package demo.oldjobs.ota

import java.sql.{DriverManager, Types}
import java.time.{Instant, OffsetDateTime, ZoneOffset}

final case class DbConfig(host: String, port: Int, name: String, user: String, password: String) {
  def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$name"
  override def toString: String = s"DbConfig($host:$port/$name, user=$user, password=***)"
}

object DbConfig {
  def fromEnv(env: Map[String, String]): Either[String, DbConfig] = {
    val port = env.getOrElse("SERVICE_DB_PORT", "5432")
    env.get("SERVICE_DB_PASSWORD").filter(_.nonEmpty) match {
      case None => Left("SERVICE_DB_PASSWORD is not set")
      case Some(_) if port.isEmpty || !port.forall(_.isDigit) || port.length > 5 => Left("SERVICE_DB_PORT is not a port number")
      case Some(pw) =>
        Right(DbConfig(env.getOrElse("SERVICE_DB_HOST", "localhost"), port.toInt,
          env.getOrElse("SERVICE_DB_NAME", "service_db"), env.getOrElse("SERVICE_DB_USER", "service"), pw))
    }
  }
}

/** Upsert into vehicle_ota_status, one row per (vin, campaign_id, ecu):
  *  - an event older than the stored row (by event_time) is ignored;
  *  - a row in a terminal state (SUCCEEDED, ROLLED_BACK) is final, later events are ignored;
  *  - replaying an already-applied event is a no-op (updated_at unchanged).
  * Rows must arrive in (event_time, partition, offset) order per key. */
object OtaDb {
  val WrittenBy = "legacy-ota-updates"
  val UpsertSql: String =
    """INSERT INTO vehicle_ota_status
      |  (vin, campaign_id, ecu, state, from_version, to_version, package_sha256, progress_pct, failure_code,
      |   event_time, schema_v, written_by)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT (vin, campaign_id, ecu) DO UPDATE SET
      |  state = EXCLUDED.state, from_version = EXCLUDED.from_version, to_version = EXCLUDED.to_version,
      |  package_sha256 = EXCLUDED.package_sha256, progress_pct = EXCLUDED.progress_pct,
      |  failure_code = EXCLUDED.failure_code, event_time = EXCLUDED.event_time, schema_v = EXCLUDED.schema_v,
      |  written_by = EXCLUDED.written_by, updated_at = now()
      |WHERE EXCLUDED.event_time >= vehicle_ota_status.event_time
      |  AND vehicle_ota_status.state NOT IN ('SUCCEEDED', 'ROLLED_BACK')
      |  AND (vehicle_ota_status.state, vehicle_ota_status.from_version, vehicle_ota_status.to_version,
      |       vehicle_ota_status.package_sha256, vehicle_ota_status.progress_pct, vehicle_ota_status.failure_code,
      |       vehicle_ota_status.event_time, vehicle_ota_status.schema_v, vehicle_ota_status.written_by)
      |      IS DISTINCT FROM
      |      (EXCLUDED.state, EXCLUDED.from_version, EXCLUDED.to_version, EXCLUDED.package_sha256,
      |       EXCLUDED.progress_pct, EXCLUDED.failure_code, EXCLUDED.event_time, EXCLUDED.schema_v,
      |       EXCLUDED.written_by)""".stripMargin

  def upsert(cfg: DbConfig, rows: Iterator[Processed]): Int = {
    if (!rows.hasNext) return 0
    val conn = DriverManager.getConnection(cfg.jdbcUrl, cfg.user, cfg.password)
    try {
      conn.setAutoCommit(false)
      val st = conn.prepareStatement(UpsertSql)
      var n = 0
      rows.foreach { r =>
        st.setString(1, r.vin)
        st.setString(2, r.campaignId)
        st.setString(3, r.ecu)
        st.setString(4, r.state)
        st.setString(5, r.fromVersion)
        st.setString(6, r.toVersion)
        st.setString(7, r.packageSha256)
        r.progressPct match {
          case Some(v) => st.setInt(8, v)
          case None    => st.setNull(8, Types.INTEGER)
        }
        st.setString(9, r.failureCode.orNull)
        st.setObject(10, OffsetDateTime.ofInstant(Instant.ofEpochMilli(r.eventTimeMs), ZoneOffset.UTC))
        st.setInt(11, OtaNormalizer.SchemaV)
        st.setString(12, WrittenBy)
        st.addBatch()
        n += 1
        if (n % 500 == 0) st.executeBatch()
      }
      st.executeBatch()
      conn.commit()
      n
    } finally conn.close()
  }
}
