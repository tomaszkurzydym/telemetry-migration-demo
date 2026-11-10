package demo.oldjobs.maintenance

import demo.oldjobs.maintenance.MaintenanceNormalizer._
import org.scalatest.funsuite.AnyFunSuite

/** Pins the record-level edges a port must reproduce: quarantine shape, key handling, ids carried on rejects. */
class MaintenanceQuarantineCharacterizationSpec extends AnyFunSuite {
  private val ok = """{"vin":"SYN-VIN-0001","work_order_id":"WO-00000001","ts_ms":1,"status":"OPEN","service_center_id":"SC-001","odometer":1,"odometer_unit":"km"}"""
  private def b(s: String) = s.getBytes("UTF-8")
  private def rej(key: Option[String], s: String) = validate(key, b(s)).asInstanceOf[Rejected]

  test("quarantine record shape: schema_v, error_code, ids, source, payload string") {
    val raw = ok.replace("\"OPEN\"", "\"open\"")
    val q = mapper.readTree(quarantine(rej(Some("SYN-VIN-0001"), raw), b(raw), "maintenance.events.raw", 2, 7L))
    assert(q.get("schema_v").asInt == 1 && q.get("error_code").asText == "INVALID_STATUS")
    assert(q.get("work_order_id").asText == "WO-00000001" && q.get("vin").asText == "SYN-VIN-0001")
    assert(q.get("source").toString == """{"topic":"maintenance.events.raw","partition":2,"offset":7}""")
    assert(mapper.readTree(q.get("payload").asText).get("status").asText == "open")
  }

  test("malformed payload is not retained; ids are null") {
    val q = mapper.readTree(quarantine(rej(None, "{oops"), b("{oops"), "t", 0, 0L))
    assert(q.get("payload").asText == "[MALFORMED: 5 bytes, not retained]")
    assert(q.get("vin").isNull && q.get("work_order_id").isNull)
  }

  test("invalid work_order_id is not carried on the reject; vin is carried as given") {
    val r = rej(Some("SYN-VIN-0001"), ok.replace("WO-00000001", "WO-1"))
    assert(r.code == "INVALID_WORK_ORDER" && r.workOrderId.isEmpty && r.vin.contains("SYN-VIN-0001"))
  }

  test("null Kafka key; empty string counts as present except for vin") {
    assert(rej(None, ok).code == "KEY_MISMATCH")
    // An empty string is "present", so it fails the field's own check, except vin, which is required non-empty.
    assert(rej(Some("SYN-VIN-0001"), ok.replace("\"SC-001\"", "\"\"")).code == "INVALID_SERVICE_CENTER")
    assert(rej(Some(""), ok.replace("\"SYN-VIN-0001\"", "\"\"")).code == "MISSING_REQUIRED_FIELD")
    assert(rej(Some("SYN-VIN-0001"), ok.replace("\"SC-001\"", "null")).code == "MISSING_REQUIRED_FIELD")
  }

  test("status and unit are case-sensitive; ts_ms must be an integer literal") {
    assert(rej(Some("SYN-VIN-0001"), ok.replace("\"km\"", "\"KM\"")).code == "INVALID_UNIT")
    assert(rej(Some("SYN-VIN-0001"), ok.replace("\"ts_ms\":1", "\"ts_ms\":1.0")).code == "INVALID_TS")
    assert(rej(Some("SYN-VIN-0001"), ok.replace("\"ts_ms\":1", "\"ts_ms\":253402300800000")).code == "INVALID_TS")
  }

  test("unknown fields are ignored; non-string technician_notes are still redacted in quarantine") {
    val raw = ok.replace("}", ""","extra":1,"labor_min":-1,"technician_notes":{"t":"SYN-NOTE-9"}}""")
    val q = quarantine(rej(Some("SYN-VIN-0001"), raw), b(raw), "t", 0, 0L)
    assert(!q.contains("SYN-NOTE") && q.contains("INVALID_LABOR"))
    assert(validate(Some("SYN-VIN-0001"), b(ok.replace("}", ""","extra":1}"""))).isInstanceOf[Valid])
  }

  test("invalid UTF-8 is MALFORMED_JSON") {
    assert(validate(Some("SYN-VIN-0001"), Array(0x7b, 0xff, 0x7d).map(_.toByte)).asInstanceOf[Rejected].code == "MALFORMED_JSON")
  }
}
