package demo.oldjobs.maintenance

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.sql.DriverManager
import scala.io.Source
import scala.util.Try

/** Pins the service_records write rules against the local service-db (SYNTHETIC DEMO). Cancelled when the database is
  * not configured or not reachable. Uses one reserved work order and removes it afterwards. */
class ServiceDbSpec extends AnyFunSuite {
  private val Wo = "WO-99999999"

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
    DbConfig.fromEnv(fileVars ++ sys.env).toOption
      .filter(c => Try(DriverManager.getConnection(c.jdbcUrl, c.user, c.password).close()).isSuccess)
  }

  private def db: DbConfig = cfg.getOrElse(cancel("service-db not configured or not reachable"))
  private def sql[T](f: java.sql.Connection => T): T = {
    val c = DriverManager.getConnection(db.jdbcUrl, db.user, db.password)
    try f(c) finally c.close()
  }
  private def row(): Option[(String, String, java.sql.Timestamp)] = sql { c =>
    val st = c.prepareStatement("select status, odometer_km::text, updated_at from service_records where work_order_id = ?")
    st.setString(1, Wo)
    val rs = st.executeQuery()
    if (rs.next()) Some((rs.getString(1), rs.getString(2), rs.getTimestamp(3))) else None
  }
  private def ev(status: String, ts: Long, km: String) =
    Processed(valid = true, Wo, "SYN-VIN-0001", status, "SC-001", ts, km, Seq("P0A80"), 2, Some(30), null, 0, 0L)
  private def write(p: Processed*): Int = ServiceDb.upsert(db, p.iterator)
  private def clear(): Unit = sql { c =>
    val st = c.prepareStatement("delete from service_records where work_order_id = ?"); st.setString(1, Wo); st.executeUpdate()
  }

  test("upsert: newer event wins, older event ignored, identical replay leaves updated_at unchanged") {
    clear()
    try {
      write(ev("OPEN", 1000L, "100.0"))
      write(ev("COMPLETED", 2000L, "100.5"))
      val first = row().get
      assert(first._1 == "COMPLETED" && first._2 == "100.5")

      write(ev("IN_PROGRESS", 1500L, "999.0"))
      assert(row().get == first)

      Thread.sleep(20)
      write(ev("COMPLETED", 2000L, "100.5"))
      assert(row().get == first)

      write(ev("COMPLETED", 2000L, "100.7"))
      val changed = row().get
      assert(changed._2 == "100.7" && changed._3.after(first._3))
    } finally clear()
  }
}
