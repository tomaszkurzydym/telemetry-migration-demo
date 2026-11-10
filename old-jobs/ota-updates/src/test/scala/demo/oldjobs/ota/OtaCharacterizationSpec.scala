package demo.oldjobs.ota

import demo.oldjobs.ota.OtaNormalizer._
import org.scalatest.funsuite.AnyFunSuite

/** Pins the legacy job's current behaviour, defects included (SYNTHETIC DEMO). */
class OtaCharacterizationSpec extends AnyFunSuite {
  private val Vin = "SYN-VIN-0001"
  private val Sha = "ab" * 32
  private def ev(extra: String = "", state: String = "DOWNLOADING", from: String = "4.1.3", to: String = "4.2.0",
                 drop: Set[String] = Set.empty, set: Map[String, String] = Map.empty): Array[Byte] = {
    val base = Seq(
      "vin" -> s""""$Vin"""", "campaign_id" -> "\"OTA-2026-0001\"", "ts_ms" -> "1767225600000", "ecu" -> "\"BMS\"",
      "state" -> s""""$state"""", "from_version" -> s""""$from"""", "to_version" -> s""""$to"""",
      "package_sha256" -> s""""$Sha"""", "session_token" -> "\"SYN-TOK-00000001\"")
    val fields = base.filterNot(f => drop(f._1)).map { case (k, v) => s""""$k":${set.getOrElse(k, v)}""" } ++
      Option(extra).filter(_.nonEmpty)
    fields.mkString("{", ",", "}").getBytes("UTF-8")
  }
  private def valid(b: Array[Byte]): Row = validate(Some(Vin), b) match {
    case Valid(r) => r
    case other    => fail(s"expected Valid, got $other")
  }
  private def code(b: Array[Byte], key: Option[String] = Some(Vin)): String = validate(key, b) match {
    case Rejected(c, _, _, _) => c
    case other                => fail(s"expected Rejected, got $other")
  }

  test("DEFECT-O1: versions are compared as strings, so 1.9.4 -> 1.10.2 is rejected as a downgrade") {
    assert(code(ev(from = "1.9.4", to = "1.10.2")) == "DOWNGRADE_REJECTED") // correct: accepted (upgrade)
    assert(code(ev(from = "2.9.9", to = "10.0.0")) == "DOWNGRADE_REJECTED") // correct: accepted (upgrade)
  }

  test("DEFECT-O1: a real downgrade 2.10.1 -> 2.9.0 is accepted") {
    val r = valid(ev(from = "2.10.1", to = "2.9.0")) // correct: DOWNGRADE_REJECTED
    assert(r.fromVersion == "2.10.1" && r.toVersion == "2.9.0")
  }

  test("same-width versions compare correctly; equal versions are rejected") {
    assert(valid(ev(from = "4.1.3", to = "4.2.0")).toVersion == "4.2.0")
    assert(valid(ev(from = "11.4.2", to = "12.0.1")).toVersion == "12.0.1")
    assert(code(ev(from = "4.2.0", to = "4.1.9")) == "DOWNGRADE_REJECTED")
    assert(code(ev(from = "4.2.0", to = "4.2.0")) == "DOWNGRADE_REJECTED")
  }

  test("valid event is normalized: hash lower-cased, versions kept verbatim, unknown fields and token dropped") {
    val r = valid(ev(""""bytes_downloaded":123,"progress_pct":40""", set = Map("package_sha256" -> s""""${"AB" * 32}"""")))
    assert(r == Row(Vin, "OTA-2026-0001", "BMS", "DOWNLOADING", "4.1.3", "4.2.0", Sha, Some(40), None, 1767225600000L))
  }

  test("validation order: the first failing check decides the code") {
    assert(code("{".getBytes("UTF-8")) == "MALFORMED_JSON")
    assert(code(ev(drop = Set("package_sha256"), set = Map("ecu" -> "\"XX\""))) == "MISSING_REQUIRED_FIELD")
    assert(code(ev(set = Map("ecu" -> "\"XX\"")), key = Some("SYN-VIN-0002")) == "KEY_MISMATCH")
    assert(code(ev(set = Map("campaign_id" -> "\"OTA-26-1\"", "ts_ms" -> "-1"))) == "INVALID_CAMPAIGN")
    assert(code(ev(set = Map("ts_ms" -> "-1", "ecu" -> "\"XX\""))) == "INVALID_TS")
    assert(code(ev(set = Map("ecu" -> "\"XX\"", "state" -> "\"DONE\""))) == "INVALID_ECU")
    assert(code(ev(set = Map("state" -> "\"DONE\"", "to_version" -> "\"x\""))) == "INVALID_STATE")
    assert(code(ev(to = "4.2", set = Map("package_sha256" -> "\"zz\""))) == "INVALID_VERSION")
    assert(code(ev(from = "4.2.0", to = "4.1.0", set = Map("package_sha256" -> "\"zz\""))) == "DOWNGRADE_REJECTED")
    assert(code(ev(""""progress_pct":101""", set = Map("package_sha256" -> "\"zz\""))) == "INVALID_PACKAGE_HASH")
    assert(code(ev(""""progress_pct":101,"failure_code":"E-001"""")) == "INVALID_PROGRESS")
    assert(code(ev(""""failure_code":"E-001"""")) == "INVALID_FAILURE_CODE")
  }

  test("required fields: absent or null is MISSING_REQUIRED_FIELD; empty vin too, other empty strings are invalid") {
    RequiredFields.foreach(k => assert(code(ev(drop = Set(k))) == "MISSING_REQUIRED_FIELD", k))
    assert(code(ev(set = Map("state" -> "null"))) == "MISSING_REQUIRED_FIELD")
    assert(code(ev(set = Map("vin" -> "\"\""))) == "MISSING_REQUIRED_FIELD")
    assert(code(ev(set = Map("campaign_id" -> "\"\""))) == "INVALID_CAMPAIGN")
    assert(code(ev(set = Map("ecu" -> "\"\""))) == "INVALID_ECU")
    assert(valid(ev(drop = Set("session_token"))).vin == Vin) // session_token is optional
  }

  test("key must equal vin; a missing key is KEY_MISMATCH") {
    assert(code(ev(), key = None) == "KEY_MISMATCH")
    assert(code(ev(), key = Some("syn-vin-0001")) == "KEY_MISMATCH")
  }

  test("field formats: campaign, ts_ms, ecu, state, versions, hash are exact and case-sensitive") {
    Seq("\"ota-2026-0001\"", "\"OTA-2026-001\"", "\"OTA-2026-0001 \"", "7").foreach(c =>
      assert(code(ev(set = Map("campaign_id" -> c))) == "INVALID_CAMPAIGN", c))
    Seq("1.5", "\"1767225600000\"", "253402300800000", "1e3").foreach(t =>
      assert(code(ev(set = Map("ts_ms" -> t))) == "INVALID_TS", t))
    assert(valid(ev(set = Map("ts_ms" -> "0"))).eventTimeMs == 0L)
    assert(valid(ev(set = Map("ts_ms" -> "253402300799999"))).eventTimeMs == MaxTsMs)
    assert(code(ev(set = Map("ecu" -> "\"bms\""))) == "INVALID_ECU")
    assert(code(ev(state = "succeeded")) == "INVALID_STATE")
    Seq("4.2", "4.2.0.1", "v4.2.0", "4.2.10000", "4.2.0-rc1").foreach(v =>
      assert(code(ev(to = v)) == "INVALID_VERSION", v))
    Seq("ab" * 31, "ab" * 33, "g" * 64).foreach(h =>
      assert(code(ev(set = Map("package_sha256" -> s""""$h""""))) == "INVALID_PACKAGE_HASH", h))
  }

  test("progress_pct is optional; when present it must be an integer 0..100") {
    assert(valid(ev()).progressPct.isEmpty)
    assert(valid(ev(""""progress_pct":null""")).progressPct.isEmpty)
    assert(valid(ev(""""progress_pct":0""")).progressPct.contains(0))
    assert(valid(ev(""""progress_pct":100""")).progressPct.contains(100))
    Seq("-1", "101", "50.0", "\"50\"").foreach(p => assert(code(ev(s""""progress_pct":$p""")) == "INVALID_PROGRESS", p))
  }

  test("failure_code is required for FAILED and ROLLED_BACK, and must be absent otherwise") {
    assert(valid(ev(""""failure_code":"E-042"""", state = "FAILED")).failureCode.contains("E-042"))
    assert(valid(ev(""""failure_code":"E-007"""", state = "ROLLED_BACK")).failureCode.contains("E-007"))
    assert(valid(ev(""""failure_code":null""", state = "SUCCEEDED")).failureCode.isEmpty)
    assert(code(ev(state = "FAILED")) == "INVALID_FAILURE_CODE")
    Seq("\"E-42\"", "\"e-042\"", "42").foreach(f =>
      assert(code(ev(s""""failure_code":$f""", state = "FAILED")) == "INVALID_FAILURE_CODE", f))
    assert(code(ev(""""failure_code":"E-042"""", state = "SUCCEEDED")) == "INVALID_FAILURE_CODE")
  }

  test("JSON edge cases: non-object, trailing tokens and invalid UTF-8 are MALFORMED_JSON; duplicate keys too") {
    Seq("[]", "\"x\"", "{} {}", "").foreach(s => assert(code(s.getBytes("UTF-8")) == "MALFORMED_JSON", s))
    assert(code(Array[Byte](0x7b, 0xc3.toByte, 0x28, 0x7d)) == "MALFORMED_JSON")
    assert(code(null) == "MALFORMED_JSON")
    // Jackson's default keeps the last duplicate key; pinned as current behaviour.
    assert(valid(ev(""""state":"INSTALLING"""")).state == "INSTALLING")
  }

  test("startup config: password required, port validated, password never printed") {
    assert(DbConfig.fromEnv(Map.empty).isLeft)
    assert(DbConfig.fromEnv(Map("SERVICE_DB_PASSWORD" -> "")).isLeft)
    assert(DbConfig.fromEnv(Map("SERVICE_DB_PASSWORD" -> "p", "SERVICE_DB_PORT" -> "54x")).isLeft)
    val c = DbConfig.fromEnv(Map("SERVICE_DB_PASSWORD" -> "synthetic-pw")).toOption.get
    assert(c.jdbcUrl == "jdbc:postgresql://localhost:5432/service_db" && !c.toString.contains("synthetic-pw"))
  }
}
