package demo.oldjobs.ota

import demo.oldjobs.ota.OtaNormalizer._
import org.scalatest.funsuite.AnyFunSuite

/** Pins quarantine records and log lines; session_token must never be retained (SYNTHETIC DEMO). */
class OtaQuarantineCharacterizationSpec extends AnyFunSuite {
  private val Tok = "SYN-TOK-00004242"
  private def b(s: String) = s.getBytes("UTF-8")
  private def rejected(key: String, v: Array[Byte]): Rejected = validate(Some(key), v) match {
    case r: Rejected => r
    case other       => fail(s"expected Rejected, got $other")
  }

  test("quarantine record: error code, ids, source position and payload with session_token redacted at any depth") {
    val v = b(s"""{"vin":"SYN-VIN-0007","campaign_id":"OTA-2026-0002","ecu":"VCU","ts_ms":1,"session_token":"$Tok",""" +
      s""""diagnostics":{"rssi_dbm":-71,"session_token":"$Tok","hops":[{"session_token":"$Tok"}]},"progress_pct":1.50}""")
    val r = rejected("SYN-VIN-0007", v)
    assert(r == Rejected("MISSING_REQUIRED_FIELD", Some("OTA-2026-0002"), Some("SYN-VIN-0007"), Some("VCU")))
    val q = quarantine(r, v, "ota.events.raw", 3, 17L)
    assert(!q.contains(Tok))
    val node = mapper.readTree(q)
    assert(node.get("schema_v").asInt == 1 && node.get("error_code").asText == "MISSING_REQUIRED_FIELD")
    assert(node.get("source").toString == """{"topic":"ota.events.raw","partition":3,"offset":17}""")
    assert(node.get("payload").asText ==
      """{"vin":"SYN-VIN-0007","campaign_id":"OTA-2026-0002","ecu":"VCU","ts_ms":1,"session_token":"[REDACTED]",""" +
      """"diagnostics":{"rssi_dbm":-71,"session_token":"[REDACTED]","hops":[{"session_token":"[REDACTED]"}]},"progress_pct":1.50}""")
  }

  test("ids in the quarantine record are only kept when well-formed; vin is kept as sent") {
    val r = rejected("k", b("""{"vin":"SYN-VIN-0007","campaign_id":"bad","ecu":"bms"}"""))
    assert(r == Rejected("MISSING_REQUIRED_FIELD", None, Some("SYN-VIN-0007"), None))
    val q = mapper.readTree(quarantine(r, b("{}"), "t", 0, 0L))
    assert(q.get("campaign_id").isNull && q.get("ecu").isNull && q.get("vin").asText == "SYN-VIN-0007")
  }

  test("malformed payloads are not retained, only their size") {
    val v = b(s"""{"vin":"SYN-VIN-0001","session_token":"$Tok"""")
    val r = rejected("SYN-VIN-0001", v)
    assert(r == Rejected("MALFORMED_JSON", None, None, None))
    val q = quarantine(r, v, "ota.events.raw", 0, 5L)
    assert(!q.contains(Tok) && mapper.readTree(q).get("payload").asText == s"[MALFORMED: ${v.length} bytes, not retained]")
    assert(mapper.readTree(q).get("vin").isNull)
  }

  test("a token-like value under another key is not redacted (redaction is by key name only)") {
    val v = b(s"""{"vin":"SYN-VIN-0001","note":"$Tok"}""")
    assert(quarantine(rejected("SYN-VIN-0001", v), v, "t", 0, 0L).contains(Tok))
  }

  test("log line has campaign_id, ecu and error_code only") {
    val r = Rejected("INVALID_STATE", Some("OTA-2026-0001"), Some("SYN-VIN-0001"), Some("BMS"))
    assert(logLine(r) == "quarantined campaign_id=OTA-2026-0001 ecu=BMS error_code=INVALID_STATE")
    assert(logLine(Rejected("MALFORMED_JSON", None, None, None)) == "quarantined campaign_id=- ecu=- error_code=MALFORMED_JSON")
  }
}
