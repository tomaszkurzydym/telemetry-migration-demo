package demo.legacy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

/** Legacy Spark Structured Streaming job (SYNTHETIC DEMO). Kafka sink is at-least-once. */
object LegacyJob {
  def main(args: Array[String]): Unit = {
    // Hard-fail before Spark starts (contract §5).
    val crypto = LegacyCrypto.fromEnv() match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"FATAL startup: $err; refusing to start")
        sys.exit(78)
    }
    val env = sys.env
    val bootstrap = env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val input = env.getOrElse("INPUT_TOPIC", "telemetry.battery.raw")
    val output = env.getOrElse("OUTPUT_TOPIC", "telemetry.battery.normalized")
    val quarantine = env.getOrElse("QUARANTINE_TOPIC", "telemetry.battery.quarantine")
    val checkpoint = env.getOrElse("CHECKPOINT_DIR", "target/checkpoints/legacy")
    val availableNow = env.getOrElse("TRIGGER", "available-now") == "available-now"

    val spark = SparkSession.builder()
      .appName("legacy-battery-normalizer")
      .master(env.getOrElse("SPARK_MASTER", "local[4]"))
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val raw = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", input)
      .option("startingOffsets", "earliest")
      .option("kafka.group.id", "legacy-cg")
      .option("failOnDataLoss", "false")
      .load()
      .select(col("key").cast("string").as("key"), col("value"), col("topic"), col("partition"), col("offset"))
      .as[InRec]

    val out = raw
      .groupByKey(r => Option(r.key).getOrElse(""))
      .flatMapGroupsWithState[VinState, OutRec](OutputMode.Append, GroupStateTimeout.NoTimeout) {
        (_: String, records: Iterator[InRec], state: GroupState[VinState]) =>
          val (outs, next) = VinLogic.process(state.getOption, records, crypto, output, quarantine, s => System.err.println(s))
          next.foreach(state.update)
          outs.iterator
      }

    val q = out.writeStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("kafka.enable.idempotence", "true")
      .option("checkpointLocation", checkpoint)
      .outputMode(OutputMode.Append)
      .trigger(if (availableNow) Trigger.AvailableNow() else Trigger.ProcessingTime("1 second"))
      .start()
    q.awaitTermination()
    spark.stop()
  }
}
