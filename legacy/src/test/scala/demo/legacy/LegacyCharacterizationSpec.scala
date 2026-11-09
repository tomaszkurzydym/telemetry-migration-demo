package demo.legacy

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

/** Characterization tests: pin down what the legacy job does TODAY (including DEFECT-1). */
class LegacyCharacterizationSpec extends AnyFunSuite {
  private val key = Base64.getEncoder.encodeToString(Array.tabulate[Byte](32)(i => i.toByte))
  private val crypto = LegacyCrypto.fromBase64(key, "test-key").toOption.get
  private def b(s: String) = s.getBytes(UTF_8)
  private def ev(extra: String = "", ts: String = "1767225600000") =
    s"""{"vin":"SYN-VIN-0001","event_id":"e1","ts_ms":$ts,"soc_pct":55.55,"pack_voltage_v":398.2,"pack_temp_c":25,"cell_delta_mv":12.7,"firmware":"fw-1"$extra}"""
  private def valid(s: String) = LegacyNormalizer.validate(Some("SYN-VIN-0001"), b(s)).asInstanceOf[LegacyNormalizer.Valid]
  private def code(s: String, k: Option[String] = Some("SYN-VIN-0001")) =
    LegacyNormalizer.validate(k, b(s)) match { case LegacyNormalizer.Rejected(c, _, _) => c; case _ => "VALID" }
  private def norm(s: String) = LegacyNormalizer.mapper.readTree(LegacyNormalizer.normalize(valid(s), outOfOrder = false, crypto))

  test("DEFECT-1: cell_delta_mv is truncated, not rounded") {
    assert(norm(ev()).get("cell_delta_mv").decimalValue.toPlainString == "12")
    assert(LegacyNormalizer.cellDeltaLegacy(new java.math.BigDecimal("12.99")).toPlainString == "12")
    assert(LegacyNormalizer.cellDeltaLegacy(new java.math.BigDecimal("-3.7")).toPlainString == "-3")
  }

  test("soc_pct half-up to 1dp, temp to Kelvin 2dp, ISO event_time, schema_v 2") {
    val n = norm(ev())
    assert(n.get("soc_pct").decimalValue.toPlainString == "55.6")
    assert(n.get("pack_temp_k").decimalValue.toPlainString == "298.15")
    assert(n.get("event_time").asText == "2026-01-01T00:00:00.000Z")
    assert(n.get("schema_v").asInt == 2)
    assert(!n.has("location") && !n.has("location_enc") && !n.has("pack_temp_c"))
    assert(LegacyNormalizer.halfUp(new java.math.BigDecimal("0.05"), 1).toPlainString == "0.1")
  }

  test("boundary values accepted, just-outside rejected") {
    Seq("0", "100").foreach(v => assert(code(ev().replace("55.55", v)) == "VALID"))
    Seq("-0.01", "100.01").foreach(v => assert(code(ev().replace("55.55", v)) == "OUT_OF_RANGE_SOC"))
    Seq("-40", "80").foreach(v => assert(code(ev().replace("\"pack_temp_c\":25", s""""pack_temp_c":$v""")) == "VALID"))
    Seq("-40.01", "80.01").foreach(v => assert(code(ev().replace("\"pack_temp_c\":25", s""""pack_temp_c":$v""")) == "OUT_OF_RANGE_TEMP"))
  }

  test("required fields, types, malformed JSON, key mismatch") {
    assert(code("""{"vin":"SYN-VIN-0001","ts_ms":1}""") == "MISSING_REQUIRED_FIELD")
    assert(code("""{"vin":"SYN-VIN-0001","event_id":"","ts_ms":1}""") == "MISSING_REQUIRED_FIELD")
    assert(code("""{"vin":"SYN-VIN-0001","event_id":"e","ts_ms":"1"}""") == "INVALID_TYPE")
    assert(code("""{"vin":"SYN-VIN-0001","event_id":"e","ts_ms":1.5}""") == "INVALID_TYPE")
    assert(code("""{"vin":"SYN-VIN-0001","event_id":"e","ts_ms":-1}""") == "INVALID_TS")
    assert(code("""{"vin":"SYN-VIN-0001",""") == "MALFORMED_JSON")
    assert(code("""[1,2]""") == "MALFORMED_JSON")
    assert(code(ev(), Some("SYN-VIN-0002")) == "KEY_MISMATCH")
    assert(code(ev(""","location":{"lat":91,"lon":0}""")) == "INVALID_LOCATION")
  }

  test("numbers outside the contract decimal range are INVALID_TYPE, including extreme exponents") {
    def pv(v: String) = code(ev().replace("398.2", v))
    Seq("1e30", "1e-29", "1e2147483647", "1e-2147483647", "1e9999999999", "79228162514264337593543950336").foreach(v => assert(pv(v) == "INVALID_TYPE", v))
    Seq("79228162514264337593543950335", "1.500000000000000000000000000000", "0e2147483647").foreach(v => assert(pv(v) == "VALID", v))
    assert(code(ev(""","location":{"lat":1e2147483647,"lon":0}""")) == "INVALID_LOCATION")
  }

  test("location encrypted with AAD binding, random nonce, tamper rejected") {
    val s = ev(""","location":{"lat":12.345678,"lon":-30.5}""")
    val n1 = norm(s); val n2 = norm(s)
    val e = n1.get("location_enc")
    assert(e.get("key_id").asText == "test-key")
    assert(Base64.getDecoder.decode(e.get("nonce").asText).length == 12)
    assert(e.get("nonce").asText != n2.get("location_enc").get("nonce").asText)
    val pt = new String(crypto.decrypt("SYN-VIN-0001", "e1", e.get("ciphertext").asText, e.get("nonce").asText), UTF_8)
    assert(pt == """{"lat":12.345678,"lon":-30.5}""")
    assert(!n1.toString.contains("12.345678"))
    val ct = Base64.getDecoder.decode(e.get("ciphertext").asText); ct(0) = (ct(0) ^ 1).toByte
    intercept[javax.crypto.AEADBadTagException](crypto.decrypt("SYN-VIN-0001", "e1", Base64.getEncoder.encodeToString(ct), e.get("nonce").asText))
    intercept[javax.crypto.AEADBadTagException](crypto.decrypt("SYN-VIN-0001", "e2", e.get("ciphertext").asText, e.get("nonce").asText))
  }

  test("missing key hard-fails") {
    assert(LegacyCrypto.fromEnv(Map.empty).isLeft)
    assert(LegacyCrypto.fromBase64("AAAA", "k").isLeft)
  }

  test("quarantine redacts location, including malformed payloads") {
    val bad = ev(""","location":{"lat":12.345678,"lon":-30.5}""").replace("55.55", "101")
    val r = LegacyNormalizer.validate(Some("SYN-VIN-0001"), b(bad)).asInstanceOf[LegacyNormalizer.Rejected]
    val q = LegacyNormalizer.quarantine(r, b(bad), "t", 0, 7)
    assert(q.contains("[REDACTED]") && !q.contains("12.345678") && q.contains("OUT_OF_RANGE_SOC"))
    val trunc = """{"vin":"SYN-VIN-0001","location":{"lat":12.345678,"lon":-30.5"""
    assert(!LegacyNormalizer.redactPayload(b(trunc)).contains("12.345678"))
    val nested = """{"vin":"v","lat":10.5,"extra":{"longitude":-29.5,"pts":[{"location":{"lat":1,"lon":2}}]}}"""
    assert(LegacyNormalizer.redactPayload(b(nested)) ==
      """{"vin":"v","lat":"[REDACTED]","extra":{"longitude":"[REDACTED]","pts":[{"location":"[REDACTED]"}]}}""")
  }

  test("dedupe by event_id and out_of_order flag per VIN") {
    def rec(id: String, ts: Long, off: Long) =
      InRec("SYN-VIN-0001", b(ev(ts = ts.toString).replace("\"e1\"", s""""$id"""")), "t", 0, off)
    val (out, st) = VinLogic.process(None, Iterator(rec("a", 100, 0), rec("b", 50, 1), rec("a", 200, 2), rec("c", 150, 3)), crypto, "o", "q", _ => ())
    val ooo = out.map(o => LegacyNormalizer.mapper.readTree(o.value)).map(n => n.get("event_id").asText -> n.get("out_of_order").asBoolean)
    assert(ooo == Seq("a" -> false, "b" -> true, "c" -> false))
    assert(st.get.maxTsMs == 150)
  }
}
