package demo.oldjobs.ota

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Locale
import scala.collection.JavaConverters._

/** Legacy ota-updates validation + normalization, as it behaves today (SYNTHETIC DEMO).
  *
  * DEFECT-O1 (intentional, documented): the downgrade guard compares versions as strings, not numerically. So
  * 1.9.4 → 1.10.0 is rejected as DOWNGRADE_REJECTED, while the real downgrade 1.10.0 → 1.9.4 is accepted.
  */
object OtaNormalizer {
  val SchemaV = 1
  val Ecus: Set[String] = Set("BMS", "VCU", "INFOTAINMENT", "ADAS", "TCU")
  val States: Set[String] = Set("OFFERED", "DOWNLOADING", "DOWNLOADED", "INSTALLING", "SUCCEEDED", "FAILED", "ROLLED_BACK")
  val FailureStates: Set[String] = Set("FAILED", "ROLLED_BACK")
  /** A stored row in one of these states is final: later events for the same (vin, campaign_id, ecu) are ignored. */
  val TerminalStates: Set[String] = Set("SUCCEEDED", "ROLLED_BACK")
  val MaxTsMs: Long = 253402300799999L

  private val CampaignRe = "OTA-[0-9]{4}-[0-9]{4}".r.pattern
  private val VersionRe = "[0-9]{1,4}\\.[0-9]{1,4}\\.[0-9]{1,4}".r.pattern
  private val Sha256Re = "[0-9a-fA-F]{64}".r.pattern
  private val FailureCodeRe = "E-[0-9]{3}".r.pattern
  private val TokenKey = "session_token"

  val mapper: ObjectMapper = new ObjectMapper()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
  mapper.configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)

  final case class Row(
      vin: String, campaignId: String, ecu: String, state: String, fromVersion: String, toVersion: String,
      packageSha256: String, progressPct: Option[Int], failureCode: Option[String], eventTimeMs: Long)

  sealed trait Result
  final case class Valid(row: Row) extends Result
  final case class Rejected(code: String, campaignId: Option[String], vin: Option[String], ecu: Option[String]) extends Result

  private def strictUtf8(b: Array[Byte]): Option[String] =
    try Some(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(b)).toString)
    catch { case _: java.nio.charset.CharacterCodingException => None }

  def parseObject(value: Array[Byte]): Option[ObjectNode] =
    Option(value).flatMap(strictUtf8).flatMap { s =>
      try mapper.readTree(s) match {
        case o: ObjectNode => Some(o)
        case _             => None
      } catch { case _: Exception => None }
    }

  private def present(o: ObjectNode, k: String): Option[JsonNode] = Option(o.get(k)).filterNot(_.isNull)
  private def text(o: ObjectNode, k: String): Option[String] =
    Option(o.get(k)).filter(_.isTextual).map(_.asText).filter(_.nonEmpty)
  private def intIn(n: JsonNode, lo: Long, hi: Long): Option[BigInteger] =
    Some(n).filter(_.isIntegralNumber).map(_.bigIntegerValue)
      .filter(b => b.compareTo(BigInteger.valueOf(lo)) >= 0 && b.compareTo(BigInteger.valueOf(hi)) <= 0)
  def isCampaign(s: String): Boolean = CampaignRe.matcher(s).matches

  val RequiredFields = Seq("vin", "campaign_id", "ts_ms", "ecu", "state", "from_version", "to_version", "package_sha256")

  /** DEFECT-O1: plain string comparison ("1.10.0" < "1.9.4"). */
  def isUpgrade(from: String, to: String): Boolean = to.compareTo(from) > 0

  /** Checks run in this order; the first failing check decides the error code. */
  def validate(key: Option[String], value: Array[Byte]): Result = {
    val o = parseObject(value).getOrElse(return Rejected("MALFORMED_JSON", None, None, None))
    val vinRaw = text(o, "vin")
    def reject(code: String) =
      Rejected(code, text(o, "campaign_id").filter(isCampaign), vinRaw, text(o, "ecu").filter(Ecus))

    if (RequiredFields.exists(k => present(o, k).isEmpty)) return reject("MISSING_REQUIRED_FIELD")
    val vin = vinRaw.getOrElse(return reject("MISSING_REQUIRED_FIELD"))
    if (!key.contains(vin)) return reject("KEY_MISMATCH")
    val campaign = text(o, "campaign_id").filter(isCampaign).getOrElse(return reject("INVALID_CAMPAIGN"))
    val ts = intIn(o.get("ts_ms"), 0, MaxTsMs).map(_.longValue).getOrElse(return reject("INVALID_TS"))
    val ecu = text(o, "ecu").filter(Ecus).getOrElse(return reject("INVALID_ECU"))
    val state = text(o, "state").filter(States).getOrElse(return reject("INVALID_STATE"))
    val from = text(o, "from_version").filter(VersionRe.matcher(_).matches).getOrElse(return reject("INVALID_VERSION"))
    val to = text(o, "to_version").filter(VersionRe.matcher(_).matches).getOrElse(return reject("INVALID_VERSION"))
    if (!isUpgrade(from, to)) return reject("DOWNGRADE_REJECTED")
    val sha = text(o, "package_sha256").filter(Sha256Re.matcher(_).matches).map(_.toLowerCase(Locale.ROOT))
      .getOrElse(return reject("INVALID_PACKAGE_HASH"))
    val progress = present(o, "progress_pct") match {
      case None    => None
      case Some(n) => Some(intIn(n, 0, 100).map(_.intValue).getOrElse(return reject("INVALID_PROGRESS")))
    }
    val failure = (FailureStates(state), present(o, "failure_code")) match {
      case (false, None) => None
      case (true, Some(n)) if n.isTextual && FailureCodeRe.matcher(n.asText).matches => Some(n.asText)
      case _ => return reject("INVALID_FAILURE_CODE")
    }
    Valid(Row(vin, campaign, ecu, state, from, to, sha, progress, failure, ts))
  }

  private def redactTree(n: JsonNode): Unit = n match {
    case o: ObjectNode =>
      o.fieldNames.asScala.toList.foreach { k =>
        if (k == TokenKey) o.set[JsonNode](k, TextNode.valueOf("[REDACTED]")) else redactTree(o.get(k))
      }
    case a if a.isArray => a.elements.asScala.foreach(redactTree)
    case _              =>
  }

  /** Parseable payloads are kept with session_token redacted; unparseable payloads are not retained. */
  def redactPayload(value: Array[Byte]): String = parseObject(value) match {
    case Some(o) => redactTree(o); mapper.writeValueAsString(o)
    case None    => s"[MALFORMED: ${Option(value).map(_.length).getOrElse(0)} bytes, not retained]"
  }

  def quarantine(r: Rejected, value: Array[Byte], topic: String, partition: Int, offset: Long): String = {
    val q = mapper.createObjectNode()
    q.put("schema_v", SchemaV)
    q.put("error_code", r.code)
    r.campaignId.fold(q.putNull("campaign_id"))(q.put("campaign_id", _))
    r.vin.fold(q.putNull("vin"))(q.put("vin", _))
    r.ecu.fold(q.putNull("ecu"))(q.put("ecu", _))
    val src = q.putObject("source")
    src.put("topic", topic); src.put("partition", partition); src.put("offset", offset)
    q.put("payload", redactPayload(value))
    mapper.writeValueAsString(q)
  }

  /** Failure log line: campaign_id, ecu and error_code only, never payloads or tokens. */
  def logLine(r: Rejected): String =
    s"quarantined campaign_id=${r.campaignId.getOrElse("-")} ecu=${r.ecu.getOrElse("-")} error_code=${r.code}"
}
