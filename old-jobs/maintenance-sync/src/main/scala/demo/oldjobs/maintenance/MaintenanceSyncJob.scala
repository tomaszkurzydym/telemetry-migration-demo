package demo.oldjobs.maintenance

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

final case class InRec(key: String, value: Array[Byte], topic: String, partition: Int, offset: Long)

/** One input record after validation: a service_records row, or a quarantine record. */
final case class Processed(
    valid: Boolean, workOrderId: String, vin: String, status: String, serviceCenterId: String, eventTimeMs: Long,
    odometerKm: String, dtcCodes: Seq[String], partsCount: Int, laborMin: Option[Int], quarantineJson: String,
    partition: Int, offset: Long)

object Processed {
  def of(r: InRec, log: String => Unit): Processed =
    MaintenanceNormalizer.validate(Option(r.key), r.value) match {
      case MaintenanceNormalizer.Valid(w) =>
        Processed(valid = true, w.workOrderId, w.vin, w.status, w.serviceCenterId, w.eventTimeMs,
          w.odometerKm.toPlainString, w.dtcCodes, w.partsCount, w.laborMin, null, r.partition, r.offset)
      case rej: MaintenanceNormalizer.Rejected =>
        log(MaintenanceNormalizer.logLine(rej))
        Processed(valid = false, rej.workOrderId.orNull, rej.vin.orNull, null, null, 0L, null, Seq.empty, 0, None,
          MaintenanceNormalizer.quarantine(rej, r.value, r.topic, r.partition, r.offset), r.partition, r.offset)
    }
}

/** Legacy Spark Structured Streaming job (SYNTHETIC DEMO): Kafka maintenance events → Postgres service database.
  * Delivery: at-least-once per micro-batch. DB writes are idempotent upserts; quarantine records may repeat on replay. */
object MaintenanceSyncJob {
  def main(args: Array[String]): Unit = {
    val env = sys.env
    val db = DbConfig.fromEnv(env) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"FATAL startup: $err; refusing to start")
        sys.exit(78)
    }
    val bootstrap = env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val input = env.getOrElse("MAINTENANCE_INPUT_TOPIC", "maintenance.events.raw")
    val quarantine = env.getOrElse("MAINTENANCE_QUARANTINE_TOPIC", "maintenance.events.quarantine")
    val checkpoint = env.getOrElse("CHECKPOINT_DIR", "target/checkpoints/maintenance-sync")
    val availableNow = env.getOrElse("TRIGGER", "available-now") == "available-now"

    val spark = SparkSession.builder()
      .appName("legacy-maintenance-sync")
      .master(env.getOrElse("SPARK_MASTER", "local[4]"))
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val processed = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", input)
      .option("startingOffsets", "earliest")
      .option("kafka.group.id", "maintenance-legacy-cg")
      .option("failOnDataLoss", "false")
      .load()
      .select(col("key").cast("string").as("key"), col("value"), col("topic"), col("partition"), col("offset"))
      .as[InRec]
      .map(r => Processed.of(r, s => System.err.println(s)))

    val sink: (Dataset[Processed], Long) => Unit = (batch, batchId) => {
      batch.persist()
      batch.filter(_.valid)
        .repartition(col("workOrderId"))
        .sortWithinPartitions("eventTimeMs", "partition", "offset")
        .rdd.foreachPartition(rows => ServiceDb.upsert(db, rows))
      batch.filter(!_.valid)
        .select(col("vin").as("key"), col("quarantineJson").as("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", quarantine)
        .save()
      batch.unpersist()
      System.err.println(s"batch=$batchId committed")
    }

    processed.writeStream
      .option("checkpointLocation", checkpoint)
      .trigger(if (availableNow) Trigger.AvailableNow() else Trigger.ProcessingTime("1 second"))
      .foreachBatch(sink)
      .start()
      .awaitTermination()
    spark.stop()
  }
}
