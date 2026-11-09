package demo.legacy

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.math.{BigDecimal => JBigDecimal, RoundingMode}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import scala.collection.JavaConverters._

/** Legacy validation + normalization (contract §3–§4), as it behaves today.
  *
  * DEFECT-1 (intentional, documented): cell_delta_mv is TRUNCATED toward zero instead of
  * rounded half-up. Kept on purpose so the shadow diff surfaces it; fixed in the Rust port.
  */
object LegacyNormalizer {
  val SchemaV = 2
  val MaxTsMs: BigInt = BigInt("253402300799999")

  val mapper: ObjectMapper = new ObjectMapper()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
  mapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))

  private val EventTimeFmt = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  sealed trait Result
  final case class Valid(
      vin: String, eventId: String, tsMs: Long,
      socPct: Option[JBigDecimal], packVoltageV: Option[JBigDecimal], packTempC: Option[JBigDecimal],
      cellDeltaMv: Option[JBigDecimal], firmware: Option[String], location: Option[(JBigDecimal, JBigDecimal)]
  ) extends Result
  final case class Rejected(code: String, vin: Option[String], eventId: Option[String]) extends Result

  private def strictUtf8(b: Array[Byte]): Option[String] =
    try Some(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b)).toString)
    catch { case _: java.nio.charset.CharacterCodingException => None }

  def parseObject(value: Array[Byte]): Option[ObjectNode] =
    strictUtf8(value).flatMap { s =>
      try mapper.readTree(s) match {
        case o: ObjectNode => Some(o)
        case _             => None
      } catch { case _: Exception => None }
    }

  private def present(o: ObjectNode, k: String): Option[JsonNode] =
    Option(o.get(k)).filterNot(_.isNull)

  private def strField(o: ObjectNode, k: String): Option[String] =
    Option(o.get(k)).filter(_.isTextual).map(_.asText).filter(_.nonEmpty)

  private val MaxCoefficient = java.math.BigInteger.ONE.shiftLeft(96).subtract(java.math.BigInteger.ONE)

  /** Contract §3 decimal range: ±m × 10^-s with m ≤ 2^96 − 1 and s ≤ 28, after dropping trailing zeros. */
  def inDecimalRange(d: JBigDecimal): Boolean =
    d.signum == 0 || {
      val s = d.stripTrailingZeros
      if (s.scale > 28) false
      else if (s.precision.toLong - s.scale > 29) false
      else s.setScale(math.max(s.scale, 0)).unscaledValue.abs.compareTo(MaxCoefficient) <= 0
    }

  /** Exact value of a JSON number within the contract decimal range; None if outside it or not representable. */
  private def decimalOf(n: JsonNode): Option[JBigDecimal] =
    if (!n.isNumber) None
    else scala.util.Try(n.decimalValue).toOption.filter(inDecimalRange)

  def validate(key: Option[String], value: Array[Byte]): Result = {
    val o = parseObject(value).getOrElse(return Rejected("MALFORMED_JSON", None, None))
    def reject(code: String) = Rejected(code, strField(o, "vin"), strField(o, "event_id"))

    val ids = Seq("vin", "event_id").map { k =>
      present(o, k) match {
        case None                                  => return reject("MISSING_REQUIRED_FIELD")
        case Some(n) if n.isTextual && n.asText.isEmpty => return reject("MISSING_REQUIRED_FIELD")
        case Some(n) if n.isTextual                => n.asText
        case Some(_)                               => return reject("INVALID_TYPE")
      }
    }
    val ts: BigInt = present(o, "ts_ms") match {
      case None                                  => return reject("MISSING_REQUIRED_FIELD")
      case Some(n) if n.isTextual && n.asText.isEmpty => return reject("MISSING_REQUIRED_FIELD")
      case Some(n) if n.isIntegralNumber         => BigInt(n.bigIntegerValue)
      case Some(_)                               => return reject("INVALID_TYPE")
    }
    val nums = Seq("soc_pct", "pack_voltage_v", "pack_temp_c", "cell_delta_mv").map { k =>
      present(o, k) match {
        case None                    => None
        case Some(n) if decimalOf(n).isDefined => decimalOf(n)
        case Some(_)                 => return reject("INVALID_TYPE")
      }
    }
    val firmware = present(o, "firmware") match {
      case None                    => None
      case Some(n) if n.isTextual => Some(n.asText)
      case Some(_)                 => return reject("INVALID_TYPE")
    }
    val location = present(o, "location") match {
      case None => None
      case Some(l: ObjectNode) =>
        (Option(l.get("lat")), Option(l.get("lon"))) match {
          case (Some(a), Some(b)) if decimalOf(a).isDefined && decimalOf(b).isDefined =>
            Some((decimalOf(a).get, decimalOf(b).get))
          case _                                              => return reject("INVALID_LOCATION")
        }
      case Some(_) => return reject("INVALID_LOCATION")
    }
    val (vin, eventId) = (ids(0), ids(1))
    if (!key.contains(vin)) return reject("KEY_MISMATCH")
    if (ts < 0 || ts > MaxTsMs) return reject("INVALID_TS")
    nums(0).foreach { s => if (s.compareTo(JBigDecimal.ZERO) < 0 || s.compareTo(new JBigDecimal(100)) > 0) return reject("OUT_OF_RANGE_SOC") }
    nums(2).foreach { t => if (t.compareTo(new JBigDecimal(-40)) < 0 || t.compareTo(new JBigDecimal(80)) > 0) return reject("OUT_OF_RANGE_TEMP") }
    location.foreach { case (lat, lon) =>
      if (lat.abs.compareTo(new JBigDecimal(90)) > 0 || lon.abs.compareTo(new JBigDecimal(180)) > 0) return reject("INVALID_LOCATION")
    }
    Valid(vin, eventId, ts.toLong, nums(0), nums(1), nums(2), nums(3), firmware, location)
  }

  def eventTime(tsMs: Long): String = EventTimeFmt.format(Instant.ofEpochMilli(tsMs))

  def halfUp(d: JBigDecimal, scale: Int): JBigDecimal = d.setScale(scale, RoundingMode.HALF_UP)

  /** DEFECT-1: truncation (RoundingMode.DOWN) where the contract requires HALF_UP. */
  def cellDeltaLegacy(d: JBigDecimal): JBigDecimal = d.setScale(0, RoundingMode.DOWN)

  def locationPlaintext(lat: JBigDecimal, lon: JBigDecimal): Array[Byte] = {
    val n = mapper.createObjectNode()
    n.put("lat", lat)
    n.put("lon", lon)
    mapper.writeValueAsBytes(n)
  }

  def normalize(v: Valid, outOfOrder: Boolean, crypto: LegacyCrypto): String = {
    val n = mapper.createObjectNode()
    n.put("schema_v", SchemaV)
    n.put("vin", v.vin)
    n.put("event_id", v.eventId)
    n.put("event_time", eventTime(v.tsMs))
    def putDec(k: String, d: Option[JBigDecimal]): Unit = d match {
      case Some(x) => n.put(k, x)
      case None    => n.putNull(k)
    }
    putDec("soc_pct", v.socPct.map(halfUp(_, 1)))
    putDec("pack_voltage_v", v.packVoltageV)
    putDec("pack_temp_k", v.packTempC.map(c => halfUp(c.add(new JBigDecimal("273.15")), 2)))
    putDec("cell_delta_mv", v.cellDeltaMv.map(cellDeltaLegacy))
    v.firmware match { case Some(f) => n.put("firmware", f); case None => n.putNull("firmware") }
    n.put("out_of_order", outOfOrder)
    v.location.foreach { case (lat, lon) =>
      val (ct, nonce, keyId) = crypto.encrypt(v.vin, v.eventId, locationPlaintext(lat, lon))
      val e = n.putObject("location_enc")
      e.put("ciphertext", ct)
      e.put("nonce", nonce)
      e.put("key_id", keyId)
    }
    mapper.writeValueAsString(n)
  }

  private val LocationRe = "\"location\"\\s*:\\s*(\\{[^}]*\\}?|\"(?:[^\"\\\\]|\\\\.)*\"?|[^,}\\]]*)".r
  private val LatLonRe = "\"(lat|lon|latitude|longitude)\"\\s*:\\s*-?[0-9][0-9.eE+\\-]*".r

  def redactRawText(s: String): String = {
    val a = LocationRe.replaceAllIn(s, java.util.regex.Matcher.quoteReplacement("\"location\":\"[REDACTED]\""))
    LatLonRe.replaceAllIn(a, m => java.util.regex.Matcher.quoteReplacement("\"" + m.group(1) + "\":\"[REDACTED]\""))
  }

  private val LocationKeys = Set("location", "lat", "lon", "latitude", "longitude")

  private def redactTree(n: JsonNode): Unit = n match {
    case o: ObjectNode =>
      o.fieldNames.asScala.toList.foreach { k =>
        if (LocationKeys(k)) o.set[JsonNode](k, TextNode.valueOf("[REDACTED]")) else redactTree(o.get(k))
      }
    case a: ArrayNode => a.elements.asScala.foreach(redactTree)
    case _ =>
  }

  def redactPayload(value: Array[Byte]): String = parseObject(value) match {
    case Some(o) =>
      redactTree(o)
      mapper.writeValueAsString(o)
    case None => redactRawText(new String(value, StandardCharsets.UTF_8))
  }

  def quarantine(r: Rejected, value: Array[Byte], topic: String, partition: Int, offset: Long): String = {
    val n = mapper.createObjectNode()
    n.put("error_code", r.code)
    r.vin match { case Some(x) => n.put("vin", x); case None => n.putNull("vin") }
    r.eventId match { case Some(x) => n.put("event_id", x); case None => n.putNull("event_id") }
    val s = n.putObject("source")
    s.put("topic", topic)
    s.put("partition", partition)
    s.put("offset", offset)
    n.put("original_payload", redactPayload(value))
    mapper.writeValueAsString(n)
  }

  def sanitizeEventId(id: Option[String]): String =
    id.map(_.filter(c => c.isLetterOrDigit && c < 128 || "_.:-".contains(c)).take(64)).getOrElse("-")
}
