package demo.oldjobs.ota

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.DriverManager
import scala.io.Source
import scala.util.Try

/** Pins the vehicle_ota_status write rules against the local service-db (SYNTHETIC DEMO). Cancelled when the database
  * is not configured, not reachable or has no vehicle_ota_status table. Uses one reserved key and removes it. */
class OtaDbSpec extends AnyFunSuite {
  private val Vin = "SYN-VIN-9999"
  private val Campaign = "OTA-9999-9999"

  private lazy val cfg: Option[DbConfig] = {
    val dotEnv = new File("../../.env")
    val fileVars =
      if (!dotEnv.isFile) Map.empty[String, String]
      else {
        val src = Source.fromFile(dotEnv, "UTF-8")
        try src.getLines().map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#") && l.contains("="))
          .map { l => val i = l.indexOf('='); l.take(i) -> l.drop(i + 1) }.toMap
        finally src.close()
      }
    DbConfig.fromEnv(fileVars ++ sys.env).toOption.filter { c =>
      Try {
        val conn = DriverManager.getConnection(c.jdbcUrl, c.user, c.password)
        try conn.createStatement().executeQuery("select 1 from vehicle_ota_status limit 1") finally conn.close()
      }.isSuccess
    }
  }

  private def db: DbConfig = cfg.getOrElse(cancel("service-db not configured, not reachable or schema missing"))
  private def sql[T](f: java.sql.Connection => T): T = {
    val c = DriverManager.getConnection(db.jdbcUrl, db.user, db.password)
    try f(c) finally c.close()
  }
  private def row(): Option[(String, Option[Int], String, java.sql.Timestamp)] = sql { c =>
    val st = c.prepareStatement(
      "select state, progress_pct, coalesce(failure_code, '-'), updated_at from vehicle_ota_status " +
        "where vin = ? and campaign_id = ? and ecu = 'BMS'")
    st.setString(1, Vin); st.setString(2, Campaign)
    val rs = st.executeQuery()
    if (rs.next()) {
      val p = rs.getInt(2)
      Some((rs.getString(1), if (rs.wasNull) None else Some(p), rs.getString(3), rs.getTimestamp(4)))
    } else None
  }
  private def ev(state: String, ts: Long, progress: Option[Int] = None, failure: Option[String] = None) =
    Processed(valid = true, Vin, Campaign, "BMS", state, "4.1.3", "4.2.0", "ab" * 32, progress, failure, ts, null, 0, 0L)
  private def write(p: Processed*): Int = OtaDb.upsert(db, p.iterator)
  private def clear(): Unit = sql { c =>
    val st = c.prepareStatement("delete from vehicle_ota_status where vin = ? and campaign_id = ?")
    st.setString(1, Vin); st.setString(2, Campaign); st.executeUpdate()
  }

  test("upsert: newer event wins, older ignored, identical replay leaves updated_at unchanged, change updates it") {
    clear()
    try {
      write(ev("OFFERED", 1000L))
      write(ev("DOWNLOADING", 2000L, Some(40)))
      val first = row().get
      assert(first._1 == "DOWNLOADING" && first._2.contains(40))

      write(ev("OFFERED", 1500L))
      assert(row().get == first)

      Thread.sleep(20)
      write(ev("DOWNLOADING", 2000L, Some(40)))
      assert(row().get == first)

      write(ev("DOWNLOADING", 2000L, Some(41)))
      val tie = row().get
      assert(tie._2.contains(41) && tie._4.after(first._4))
    } finally clear()
  }

  test("FAILED is not final (retry continues); SUCCEEDED and ROLLED_BACK are final even for newer events") {
    clear()
    try {
      write(ev("FAILED", 1000L, failure = Some("E-042")))
      write(ev("DOWNLOADING", 2000L, Some(10)))
      assert(row().get._1 == "DOWNLOADING")
      write(ev("SUCCEEDED", 3000L, Some(100)))
      write(ev("INSTALLING", 4000L, Some(90)))
      assert(row().get._1 == "SUCCEEDED")
      clear()
      write(ev("ROLLED_BACK", 1000L, failure = Some("E-007")))
      write(ev("SUCCEEDED", 2000L))
      assert(row().get._1 == "ROLLED_BACK" && row().get._3 == "E-007")
    } finally clear()
  }

  test("within one batch rows are applied in the given order") {
    clear()
    try {
      assert(write(ev("OFFERED", 1000L), ev("DOWNLOADING", 2000L, Some(5)), ev("DOWNLOADED", 3000L, Some(100))) == 3)
      assert(row().get._1 == "DOWNLOADED")
    } finally clear()
  }
}
