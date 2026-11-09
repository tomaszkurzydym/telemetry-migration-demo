package demo.legacy

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.Base64
import scala.collection.JavaConverters._

/** Runs fixtures/golden.json against the legacy logic. Expected values are independently specified;
  * `legacy_expect_override` captures DEFECT-1 (characterized, intentional). */
class GoldenFixtureSpec extends AnyFunSuite {
  private val crypto = LegacyCrypto.fromBase64(Base64.getEncoder.encodeToString(Array.fill[Byte](32)(7)), "golden").toOption.get
  private val m = LegacyNormalizer.mapper
  private val rnd = new java.security.SecureRandom()
  private def coord(lo: Double, hi: Double) = f"${lo + rnd.nextDouble() * (hi - lo)}%.6f"
  private val coords = Map(
    "@LOC-A" -> (coord(10, 30), coord(-40, -20)),
    "@LOC-B" -> (coord(10, 30), coord(-40, -20)),
    "@LOC-INVALID" -> ("95.5", "-30.0"))

  private def same(a: JsonNode, b: JsonNode): Boolean =
    if (a.isNumber && b.isNumber) a.decimalValue.compareTo(b.decimalValue) == 0 else a == b

  private val doc = m.readTree(Files.readAllBytes(Paths.get("../fixtures/golden.json")))

  for (c <- doc.get("cases").elements.asScala) {
    test(s"golden: ${c.get("name").asText}") {
      val recs = c.get("records").elements.asScala.toVector.zipWithIndex
      val byKey = recs.groupBy(_._1.get("key").asText).toVector.sortBy(_._2.head._2)
      for ((key, group) <- byKey) {
        val inputs = group.map { case (r, i) =>
          var s = Option(r.get("input_raw")).map(_.asText).getOrElse(m.writeValueAsString(r.get("input")))
          coords.foreach { case (t, (lat, lon)) => s = s.replace("\"" + t + "\"", s"""{"lat":$lat,"lon":$lon}""") }
          InRec(key, s.getBytes(UTF_8), "golden", 0, i.toLong)
        }
        val (outs, _) = VinLogic.process(None, inputs.iterator, crypto, "out", "quar", _ => ())
        val expected = group.map(_._1).filterNot(r => Option(r.get("expect").get("duplicate")).exists(_.asBoolean))
        assert(outs.size == expected.size, s"$key: output count")
        for ((o, r) <- outs.zip(expected)) {
          val exp = r.get("expect")
          val got = m.readTree(o.value)
          coords.values.foreach { case (lat, lon) => assert(!o.value.contains(lat) && !o.value.contains(lon), "plaintext coordinate leaked") }
          if (exp.has("quarantine")) {
            assert(o.topic == "quar")
            assert(got.get("error_code").asText == exp.get("quarantine").asText)
            if (exp.has("redacted")) assert(o.value.contains("[REDACTED]"))
          } else {
            assert(o.topic == "out")
            val want = exp.get("emit").deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
            Option(r.get("legacy_expect_override")).foreach(ov => ov.fields.asScala.foreach(e => want.set[JsonNode](e.getKey, e.getValue)))
            assert(got.fieldNames.asScala.toSet - "location_enc" == want.fieldNames.asScala.toSet)
            want.fields.asScala.foreach(e => assert(same(got.get(e.getKey), e.getValue), s"${e.getKey}: ${got.get(e.getKey)} vs ${e.getValue}"))
            Option(exp.get("location_enc")).map(_.asText) match {
              case Some(tok) =>
                val e = got.get("location_enc")
                val pt = m.readTree(crypto.decrypt(key, want.get("event_id").asText, e.get("ciphertext").asText, e.get("nonce").asText))
                assert(pt.get("lat").decimalValue.compareTo(new java.math.BigDecimal(coords(tok)._1)) == 0)
                assert(pt.get("lon").decimalValue.compareTo(new java.math.BigDecimal(coords(tok)._2)) == 0)
              case None => assert(!got.has("location_enc"))
            }
          }
        }
      }
    }
  }
}
