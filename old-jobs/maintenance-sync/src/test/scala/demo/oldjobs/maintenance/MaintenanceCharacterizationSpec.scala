package demo.oldjobs.maintenance

import demo.oldjobs.maintenance.MaintenanceNormalizer._
import org.scalatest.funsuite.AnyFunSuite

/** Pins the legacy job's current behaviour, defects included (SYNTHETIC DEMO). */
class MaintenanceCharacterizationSpec extends AnyFunSuite {
  private val Vin = "SYN-VIN-0001"
  private def ev(extra: String = "", odometer: String = "1000.0", unit: String = "km", drop: Set[String] = Set.empty): Array[Byte] = {
    val base = Seq(
      "vin" -> s""""$Vin"""", "work_order_id" -> "\"WO-00000001\"", "ts_ms" -> "1767225600000",
      "status" -> "\"OPEN\"", "service_center_id" -> "\"SC-001\"", "odometer" -> odometer,
      "odometer_unit" -> s""""$unit"""")
    val fields = base.filterNot(f => drop(f._1)).map { case (k, v) => s""""$k":$v""" } ++ Option(extra).filter(_.nonEmpty)
    fields.mkString("{", ",", "}").getBytes("UTF-8")
  }
  private def valid(b: Array[Byte]): Row = validate(Some(Vin), b) match {
    case Valid(r) => r
    case other    => fail(s"expected Valid, got $other")
  }
  private def code(b: Array[Byte], key: Option[String] = Some(Vin)): String = validate(key, b) match {
    case Rejected(c, _, _) => c
    case other             => fail(s"expected Rejected, got $other")
  }

  test("DEFECT-M1: miles are converted with factor 1.6, not 1.609344") {
    assert(valid(ev(odometer = "100", unit = "mi")).odometerKm.toPlainString == "160.0") // correct: 160.9
    assert(valid(ev(odometer = "12345.6", unit = "mi")).odometerKm.toPlainString == "19753.0") // correct: 19868.5
  }

  test("DEFECT-M1: mile range check uses the wrong factor (1250000 mi accepted as 2000000.0 km)") {
    assert(valid(ev(odometer = "1250000", unit = "mi")).odometerKm.toPlainString == "2000000.0")
    assert(code(ev(odometer = "1250001", unit = "mi")) == "INVALID_ODOMETER")
  }

  test("km odometer rounded half-up to 1dp; range 0..2000000 km; at most 3 decimals") {
    assert(valid(ev(odometer = "1234.45")).odometerKm.toPlainString == "1234.5")
    assert(valid(ev(odometer = "1234.449")).odometerKm.toPlainString == "1234.4")
    assert(valid(ev(odometer = "0")).odometerKm.toPlainString == "0.0")
    assert(valid(ev(odometer = "2000000")).odometerKm.toPlainString == "2000000.0")
    assert(valid(ev(odometer = "1999999.95")).odometerKm.toPlainString == "2000000.0")
    Seq("-0.1", "2000000.1", "1.0005", "\"12\"", "1e9999999999").foreach { o =>
      assert(code(ev(odometer = o)) == "INVALID_ODOMETER", o)
    }
    // Current behaviour: Jackson refuses a scale beyond Int range while parsing, so the whole record is MALFORMED_JSON.
    assert(code(ev(odometer = "1e-9999999999")) == "MALFORMED_JSON")
  }

  test("DTC codes are trimmed, upper-cased, de-duplicated and sorted; absent means empty") {
    assert(valid(ev(""""dtc_codes":["u0100"," P0A80","U0100"]""")).dtcCodes == Seq("P0A80", "U0100"))
    assert(valid(ev()).dtcCodes.isEmpty)
    assert(code(ev(""""dtc_codes":["P0A8"]""")) == "INVALID_DTC")
    assert(code(ev(""""dtc_codes":"P0A80"""")) == "INVALID_DTC")
  }

  test("parts_count is the sum of qty; qty 1..999; absent means 0") {
    assert(valid(ev(""""parts":[{"part_no":"PN-HV0012","qty":2},{"part_no":"PN-CL0440","qty":3}]""")).partsCount == 5)
    assert(valid(ev()).partsCount == 0)
    assert(code(ev(""""parts":[{"part_no":"PN-HV0012","qty":0}]""")) == "INVALID_PARTS")
    assert(code(ev(""""parts":[{"part_no":"HV0012","qty":1}]""")) == "INVALID_PARTS")
  }

  test("labor_min optional, 0..1440") {
    assert(valid(ev(""""labor_min":1440""")).laborMin.contains(1440))
    assert(valid(ev()).laborMin.isEmpty)
    assert(code(ev(""""labor_min":1441""")) == "INVALID_LABOR")
    assert(code(ev(""""labor_min":1.5""")) == "INVALID_LABOR")
  }

  test("validation order: first failing check decides the error code") {
    assert(code("{not json".getBytes("UTF-8")) == "MALFORMED_JSON")
    assert(code("[1,2]".getBytes("UTF-8")) == "MALFORMED_JSON")
    assert(code(ev(drop = Set("status"))) == "MISSING_REQUIRED_FIELD")
    assert(code(ev(), key = Some("SYN-VIN-0002")) == "KEY_MISMATCH")
    assert(code(ev(""""labor_min":-1""", odometer = "-1")) == "INVALID_ODOMETER")
  }

  test("technician_notes never leave the job: redacted in quarantine at any depth, absent from rows and logs") {
    val raw = ev(""""technician_notes":"SYN-NOTE-000001 secret","parts":[{"part_no":"PN-HV0012","qty":0,"technician_notes":"SYN-NOTE-000002"}]""")
    val rej = validate(Some(Vin), raw).asInstanceOf[Rejected]
    val q = quarantine(rej, raw, "maintenance.events.raw", 3, 42L)
    assert(!q.contains("SYN-NOTE") && q.contains("[REDACTED]") && q.contains("\"error_code\":\"INVALID_PARTS\""))
    assert(logLine(rej) == "quarantined work_order_id=WO-00000001 error_code=INVALID_PARTS")
    assert(!valid(ev(""""technician_notes":"SYN-NOTE-000003"""")).toString.contains("SYN-NOTE"))
    assert(!redactPayload("{\"technician_notes\":\"SYN-NOTE-1\"".getBytes("UTF-8")).contains("SYN-NOTE"))
  }

  test("startup config: missing password fails; toString never prints it") {
    assert(DbConfig.fromEnv(Map.empty).isLeft)
    assert(DbConfig.fromEnv(Map("SERVICE_DB_PASSWORD" -> "pw", "SERVICE_DB_PORT" -> "x")).isLeft)
    val c = DbConfig.fromEnv(Map("SERVICE_DB_PASSWORD" -> "synthetic-pw")).toOption.get
    assert(c.jdbcUrl == "jdbc:postgresql://localhost:5432/service_db" && !c.toString.contains("synthetic-pw"))
  }
}
