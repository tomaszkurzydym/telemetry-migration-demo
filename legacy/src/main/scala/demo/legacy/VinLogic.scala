package demo.legacy

/** Per-VIN stateful logic (contract §6), used inside flatMapGroupsWithState. */
final case class VinState(maxTsMs: Long, recentEventIds: Vector[String])

final case class InRec(key: String, value: Array[Byte], topic: String, partition: Int, offset: Long)
final case class OutRec(topic: String, key: String, value: String)

object VinLogic {
  val DedupeWindow = 1000

  /** Processes one VIN's records in input-offset order. Returns outputs and the new state. */
  def process(
      state: Option[VinState], records: Iterator[InRec], crypto: LegacyCrypto,
      outputTopic: String, quarantineTopic: String, log: String => Unit
  ): (Seq[OutRec], Option[VinState]) = {
    var st = state
    val out = Vector.newBuilder[OutRec]
    records.toVector.sortBy(r => (r.partition, r.offset)).foreach { r =>
      LegacyNormalizer.validate(Option(r.key), r.value) match {
        case rej: LegacyNormalizer.Rejected =>
          log(s"quarantined event_id=${LegacyNormalizer.sanitizeEventId(rej.eventId)} error_code=${rej.code}")
          out += OutRec(quarantineTopic, r.key, LegacyNormalizer.quarantine(rej, r.value, r.topic, r.partition, r.offset))
        case v: LegacyNormalizer.Valid =>
          val dup = st.exists(_.recentEventIds.contains(v.eventId))
          if (!dup) {
            val ooo = st.exists(s => v.tsMs < s.maxTsMs)
            val prev = st.getOrElse(VinState(Long.MinValue, Vector.empty))
            st = Some(VinState(math.max(prev.maxTsMs, v.tsMs), (prev.recentEventIds :+ v.eventId).takeRight(DedupeWindow)))
            out += OutRec(outputTopic, v.vin, LegacyNormalizer.normalize(v, ooo, crypto))
          }
      }
    }
    (out.result(), st)
  }
}
