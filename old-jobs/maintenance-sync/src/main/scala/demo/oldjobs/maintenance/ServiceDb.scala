package demo.oldjobs.maintenance

import java.math.{BigDecimal => JBigDecimal}
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

/** Idempotent upsert into service_records: a replay of an already-applied event is a no-op (updated_at unchanged), and an event older than the stored
  * row (by event_time) is ignored. Rows must arrive in (event_time, partition, offset) order per work order. */
object ServiceDb {
  val WrittenBy = "legacy-maintenance-sync"
  val UpsertSql: String =
    """INSERT INTO service_records
      |  (work_order_id, vin, status, service_center_id, event_time, odometer_km, dtc_codes, parts_count, labor_min,
      |   schema_v, written_by)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT (work_order_id) DO UPDATE SET
      |  vin = EXCLUDED.vin, status = EXCLUDED.status, service_center_id = EXCLUDED.service_center_id,
      |  event_time = EXCLUDED.event_time, odometer_km = EXCLUDED.odometer_km, dtc_codes = EXCLUDED.dtc_codes,
      |  parts_count = EXCLUDED.parts_count, labor_min = EXCLUDED.labor_min, schema_v = EXCLUDED.schema_v,
      |  written_by = EXCLUDED.written_by, updated_at = now()
      |WHERE EXCLUDED.event_time >= service_records.event_time
      |  AND (service_records.vin, service_records.status, service_records.service_center_id,
      |       service_records.event_time, service_records.odometer_km, service_records.dtc_codes,
      |       service_records.parts_count, service_records.labor_min, service_records.schema_v,
      |       service_records.written_by)
      |      IS DISTINCT FROM
      |      (EXCLUDED.vin, EXCLUDED.status, EXCLUDED.service_center_id, EXCLUDED.event_time, EXCLUDED.odometer_km,
      |       EXCLUDED.dtc_codes, EXCLUDED.parts_count, EXCLUDED.labor_min, EXCLUDED.schema_v, EXCLUDED.written_by)""".stripMargin

  def upsert(cfg: DbConfig, rows: Iterator[Processed]): Int = {
    if (!rows.hasNext) return 0
    val conn = DriverManager.getConnection(cfg.jdbcUrl, cfg.user, cfg.password)
    try {
      conn.setAutoCommit(false)
      val st = conn.prepareStatement(UpsertSql)
      var n = 0
      rows.foreach { r =>
        st.setString(1, r.workOrderId)
        st.setString(2, r.vin)
        st.setString(3, r.status)
        st.setString(4, r.serviceCenterId)
        st.setObject(5, OffsetDateTime.ofInstant(Instant.ofEpochMilli(r.eventTimeMs), ZoneOffset.UTC))
        st.setBigDecimal(6, new JBigDecimal(r.odometerKm))
        st.setArray(7, conn.createArrayOf("text", r.dtcCodes.map(c => c: AnyRef).toArray))
        st.setInt(8, r.partsCount)
        r.laborMin match {
          case Some(v) => st.setInt(9, v)
          case None    => st.setNull(9, Types.INTEGER)
        }
        st.setInt(10, MaintenanceNormalizer.SchemaV)
        st.setString(11, WrittenBy)
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
