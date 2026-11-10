package demo.oldjobs.maintenance

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.math.{BigInteger, RoundingMode, BigDecimal => JBigDecimal}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Locale
import scala.collection.JavaConverters._

/** Legacy maintenance-sync validation + normalization, as it behaves today (SYNTHETIC DEMO).
  *
  * DEFECT-M1 (intentional, documented): miles are converted to km with factor 1.6 instead of the
  * statute-mile factor 1.609344. This also shifts the odometer range check for mile readings.
  */
object MaintenanceNormalizer {
  val SchemaV = 1
  val Statuses: Set[String] = Set("OPEN", "IN_PROGRESS", "COMPLETED", "CANCELLED")
  val MaxOdometerKm = new JBigDecimal("2000000")
  val MaxOdometerScale = 3
  val LegacyMilesToKm = new JBigDecimal("1.6") // DEFECT-M1
  val MaxTsMs: Long = 253402300799999L
  val MaxLaborMin = 1440
  val MaxDtcCodes = 32
  val MaxParts = 100
  val MaxPartQty = 999

  private val WorkOrderRe = "WO-[0-9]{8}".r.pattern
  private val ServiceCenterRe = "SC-[0-9]{3}".r.pattern
  private val DtcRe = "[PBCU][0-9A-F]{4}".r.pattern
  private val PartNoRe = "PN-[0-9A-Z]{6}".r.pattern
  private val NotesKey = "technician_notes"

  val mapper: ObjectMapper = new ObjectMapper()
    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
    .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
  mapper.configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false)

  final case class Row(
      workOrderId: String, vin: String, status: String, serviceCenterId: String, eventTimeMs: Long,
      odometerKm: JBigDecimal, dtcCodes: Seq[String], partsCount: Int, laborMin: Option[Int])

  sealed trait Result
  final case class Valid(row: Row) extends Result
  final case class Rejected(code: String, workOrderId: Option[String], vin: Option[String]) extends Result

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
  def isWorkOrder(s: String): Boolean = WorkOrderRe.matcher(s).matches

  val RequiredFields = Seq("vin", "work_order_id", "ts_ms", "status", "service_center_id", "odometer", "odometer_unit")

  /** Checks run in this order; the first failing check decides the error code. */
  def validate(key: Option[String], value: Array[Byte]): Result = {
    val o = parseObject(value).getOrElse(return Rejected("MALFORMED_JSON", None, None))
    val vinRaw = text(o, "vin")
    def reject(code: String) = Rejected(code, text(o, "work_order_id").filter(isWorkOrder), vinRaw)

    if (RequiredFields.exists(k => present(o, k).isEmpty)) return reject("MISSING_REQUIRED_FIELD")
    val vin = vinRaw.getOrElse(return reject("MISSING_REQUIRED_FIELD"))
    if (!key.contains(vin)) return reject("KEY_MISMATCH")
    val wo = text(o, "work_order_id").filter(isWorkOrder).getOrElse(return reject("INVALID_WORK_ORDER"))
    val ts = intIn(o.get("ts_ms"), 0, MaxTsMs).map(_.longValue).getOrElse(return reject("INVALID_TS"))
    val status = text(o, "status").filter(Statuses).getOrElse(return reject("INVALID_STATUS"))
    val sc = text(o, "service_center_id").filter(ServiceCenterRe.matcher(_).matches)
      .getOrElse(return reject("INVALID_SERVICE_CENTER"))
    val unit = text(o, "odometer_unit").filter(u => u == "km" || u == "mi").getOrElse(return reject("INVALID_UNIT"))
    val km = odometerKm(o.get("odometer"), unit).getOrElse(return reject("INVALID_ODOMETER"))
    val dtcs = dtcCodes(present(o, "dtc_codes")).getOrElse(return reject("INVALID_DTC"))
    val parts = partsCount(present(o, "parts")).getOrElse(return reject("INVALID_PARTS"))
    val labor = present(o, "labor_min") match {
      case None    => None
      case Some(n) => Some(intIn(n, 0, MaxLaborMin).map(_.intValue).getOrElse(return reject("INVALID_LABOR")))
    }
    Valid(Row(wo, vin, status, sc, ts, km, dtcs, parts, labor))
  }

  /** Raw value bounded before any scaling so hostile exponents never allocate. */
  def odometerKm(n: JsonNode, unit: String): Option[JBigDecimal] =
    if (n == null || !n.isNumber) None
    else {
      val d = scala.util.Try(n.decimalValue).toOption.getOrElse(return None)
      if (d.signum < 0 || d.compareTo(MaxOdometerKm) > 0) None
      else if (d.signum != 0 && d.stripTrailingZeros.scale > MaxOdometerScale) None
      else {
        val km = (if (unit == "mi") d.multiply(LegacyMilesToKm) else d).setScale(1, RoundingMode.HALF_UP)
        if (km.compareTo(MaxOdometerKm) > 0) None else Some(km)
      }
    }

  /** Upper-cased, trimmed, de-duplicated and sorted. Absent → empty. */
  def dtcCodes(n: Option[JsonNode]): Option[Seq[String]] = n match {
    case None => Some(Seq.empty)
    case Some(a) if a.isArray && a.size <= MaxDtcCodes =>
      val codes = a.elements.asScala.toList.map(e => Some(e).filter(_.isTextual).map(_.asText.trim.toUpperCase(Locale.ROOT)))
      if (codes.forall(_.exists(DtcRe.matcher(_).matches))) Some(codes.flatten.distinct.sorted) else None
    case _ => None
  }

  /** Sum of `qty` over `parts`. Absent → 0. */
  def partsCount(n: Option[JsonNode]): Option[Int] = n match {
    case None => Some(0)
    case Some(a) if a.isArray && a.size <= MaxParts =>
      val qtys = a.elements.asScala.toList.map {
        case p: ObjectNode if text(p, "part_no").exists(PartNoRe.matcher(_).matches) =>
          Option(p.get("qty")).flatMap(intIn(_, 1, MaxPartQty)).map(_.intValue)
        case _ => None
      }
      if (qtys.forall(_.isDefined)) Some(qtys.flatten.sum) else None
    case _ => None
  }

  private def redactTree(n: JsonNode): Unit = n match {
    case o: ObjectNode =>
      o.fieldNames.asScala.toList.foreach { k =>
        if (k == NotesKey) o.set[JsonNode](k, TextNode.valueOf("[REDACTED]")) else redactTree(o.get(k))
      }
    case a if a.isArray => a.elements.asScala.foreach(redactTree)
    case _              =>
  }

  /** Parseable payloads are kept with technician_notes redacted; unparseable payloads are not retained. */
  def redactPayload(value: Array[Byte]): String = parseObject(value) match {
    case Some(o) => redactTree(o); mapper.writeValueAsString(o)
    case None    => s"[MALFORMED: ${Option(value).map(_.length).getOrElse(0)} bytes, not retained]"
  }

  def quarantine(r: Rejected, value: Array[Byte], topic: String, partition: Int, offset: Long): String = {
    val q = mapper.createObjectNode()
    q.put("schema_v", SchemaV)
    q.put("error_code", r.code)
    r.workOrderId.fold(q.putNull("work_order_id"))(q.put("work_order_id", _))
    r.vin.fold(q.putNull("vin"))(q.put("vin", _))
    val src = q.putObject("source")
    src.put("topic", topic); src.put("partition", partition); src.put("offset", offset)
    q.put("payload", redactPayload(value))
    mapper.writeValueAsString(q)
  }

  /** Failure log line: work_order_id + error_code only, never payloads or notes. */
  def logLine(r: Rejected): String = s"quarantined work_order_id=${r.workOrderId.getOrElse("-")} error_code=${r.code}"
}
