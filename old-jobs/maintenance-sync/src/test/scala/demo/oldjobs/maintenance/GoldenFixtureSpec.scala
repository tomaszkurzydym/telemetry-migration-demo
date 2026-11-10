package demo.oldjobs.maintenance

import com.fasterxml.jackson.databind.JsonNode
import demo.oldjobs.maintenance.MaintenanceNormalizer._
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.DriverManager
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Try

/** Runs the hand-written migration fixtures (migrated/maintenance-sync/fixtures/golden.json) against the legacy job.
  * Where a case documents a legacy deviation (`legacy` with a DEFECT id), the legacy expectation is used instead. */
class GoldenFixtureSpec extends AnyFunSuite {
  private val golden = mapper.readTree(new File("../../migrated/maintenance-sync/fixtures/golden.json"))
  private def opt(n: JsonNode, k: String): Option[JsonNode] = Option(n.get(k)).filterNot(_.isNull)
  private def str(n: JsonNode, k: String): Option[String] = opt(n, k).map(_.asText)

  private def bytes(c: JsonNode): Array[Byte] =
    opt(c, "value_hex").map(h => h.asText.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)
      .orElse(opt(c, "value").map(_.asText.getBytes("UTF-8"))).orNull

  private def rowJson(r: Row): Map[String, Any] = Map(
    "work_order_id" -> r.workOrderId, "vin" -> r.vin, "status" -> r.status, "service_center_id" -> r.serviceCenterId,
    "event_time_ms" -> r.eventTimeMs, "odometer_km" -> r.odometerKm.toPlainString, "dtc_codes" -> r.dtcCodes.toList,
    "parts_count" -> r.partsCount, "labor_min" -> r.laborMin)
  private def expectedRow(e: JsonNode): Map[String, Any] = Map(
    "work_order_id" -> e.get("work_order_id").asText, "vin" -> e.get("vin").asText, "status" -> e.get("status").asText,
    "service_center_id" -> e.get("service_center_id").asText, "event_time_ms" -> e.get("event_time_ms").asLong,
    "odometer_km" -> e.get("odometer_km").asText, "dtc_codes" -> e.get("dtc_codes").elements.asScala.map(_.asText).toList,
    "parts_count" -> e.get("parts_count").asInt, "labor_min" -> opt(e, "labor_min").map(_.asInt))

  test("golden cases (legacy expectations where a DEFECT is documented)") {
    val cases = golden.get("cases").elements.asScala.toList
    assert(cases.size >= 60)
    val failures = cases.flatMap { c =>
      val exp = opt(c, "legacy").getOrElse(c.get("expect"))
      val key = str(c, "key")
      val got = validate(key, bytes(c))
      val name = c.get("name").asText
      (got, opt(exp, "row"), opt(exp, "reject")) match {
        case (Valid(r), Some(er), _) if rowJson(r) == expectedRow(er) => None
        case (rj: Rejected, _, Some(eq))
            if rj.code == eq.get("error_code").asText && rj.workOrderId == str(eq, "work_order_id") &&
              rj.vin == str(eq, "vin") =>
          val q = mapper.readTree(quarantine(rj, bytes(c), "maintenance.events.raw", 0, 0L))
          val payload = q.get("payload").asText
          val payloadOk = str(c.get("expect"), "payload").forall(_ == payload) &&
            opt(c.get("expect"), "payload_json").forall(_ == mapper.readTree(payload))
          if (payloadOk && !q.toString.contains("SYN-NOTE")) None else Some(s"$name: payload $payload")
        case _ => Some(s"$name: got $got")
      }
    }
    assert(failures.isEmpty, failures.mkString("\n"))
  }

  private lazy val db: Option[DbConfig] = {
    val f = new File("../../.env")
    val vars = if (!f.isFile) Map.empty[String, String] else {
      val s = Source.fromFile(f, "UTF-8")
      try s.getLines().map(_.trim).filter(l => l.contains("=") && !l.startsWith("#"))
        .map { l => val i = l.indexOf('='); l.take(i) -> l.drop(i + 1) }.toMap finally s.close()
    }
    DbConfig.fromEnv(vars ++ sys.env).toOption
      .filter(c => Try(DriverManager.getConnection(c.jdbcUrl, c.user, c.password).close()).isSuccess)
  }

  test("golden sequences: service_records write rules (needs local service-db)") {
    val cfg = db.getOrElse(cancel("service-db not configured or not reachable"))
    val conn = DriverManager.getConnection(cfg.jdbcUrl, cfg.user, cfg.password)
    def clear(): Unit = conn.createStatement().executeUpdate("delete from service_records where work_order_id like 'WO-999901%'")
    try {
      golden.get("sequences").elements.asScala.foreach { s =>
        clear()
        val events = s.get("events").elements.asScala.toList.zipWithIndex.map { case (e, off) =>
          Processed.of(InRec(null, e.asText.getBytes("UTF-8"), "maintenance.events.raw", 0, off.toLong) match {
            case r => r.copy(key = mapper.readTree(e.asText).get("vin").asText)
          }, _ => ())
        }
        ServiceDb.upsert(cfg, events.filter(_.valid).sortBy(p => (p.eventTimeMs, p.partition, p.offset)).iterator)
        val exp = opt(s, "legacy").map(_.get("expect")).getOrElse(s.get("expect"))
        val rs = conn.createStatement().executeQuery(
          "select work_order_id, vin, status, service_center_id, (extract(epoch from event_time)*1000)::bigint, " +
            "odometer_km::text, dtc_codes, parts_count, labor_min from service_records where work_order_id like 'WO-999901%'")
        var got = Map.empty[String, Map[String, Any]]
        while (rs.next()) {
          val labor = Option(rs.getObject(9)).map(_.asInstanceOf[Integer].intValue)
          got += rs.getString(1) -> Map("work_order_id" -> rs.getString(1), "vin" -> rs.getString(2),
            "status" -> rs.getString(3), "service_center_id" -> rs.getString(4), "event_time_ms" -> rs.getLong(5),
            "odometer_km" -> rs.getString(6), "dtc_codes" -> rs.getArray(7).getArray.asInstanceOf[Array[String]].toList,
            "parts_count" -> rs.getInt(8), "labor_min" -> labor)
        }
        val want = exp.fields.asScala.map(e => e.getKey -> expectedRow(e.getValue)).toMap
        assert(got == want, s.get("name").asText)
      }
    } finally { clear(); conn.close() }
  }
}
