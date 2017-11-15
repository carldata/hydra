package carldata.hydra

import java.time._

import carldata.series.TimeSeries
import carldata.sf.core.DBImplementation
import com.datastax.driver.core.{ResultSet, ResultSetFuture, Session, SimpleStatement}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

object TimeSeriesDB {
  def apply(db: Session): TimeSeriesDB = new TimeSeriesDB(db)
}

class TimeSeriesDB(db: Session) extends DBImplementation {


  implicit def resultSetFutureToScala(f: ResultSetFuture): Future[TimeSeries[Float]] = {
    val p = Promise[TimeSeries[Float]]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success {
          val rs = r.asScala
            .map { r =>
              (parseTimestamp(r.getTimestamp(0)), r.getFloat(1))
            }.toSeq
          new TimeSeries(rs)
        }

        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }

  def getTable(id: String): IndexedSeq[(Float, Float)] = {
    val q =
      s"""
         |SELECT x, y
         |FROM lookup_table
         |WHERE id='$id'
      """.stripMargin
    val statement = new SimpleStatement(q)
    db.execute(statement).asScala.map { r =>
      (r.getFloat(0), r.getFloat(1))
    }.toIndexedSeq
  }

  def getSeries(channel: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    val t1 = from.toInstant(ZoneOffset.UTC).toEpochMilli
    val t2 = to.toInstant(ZoneOffset.UTC).toEpochMilli
    val q =
      s"""
         |SELECT timestamp, value
         |FROM data
         |WHERE channel='$channel' and timestamp >= $t1 and timestamp <= $t2
      """.stripMargin
    val statement = new SimpleStatement(q).setFetchSize(Int.MaxValue)
    resultSetFutureToScala(db.executeAsync(statement))


  }

  def parseTimestamp(dt: java.util.Date): LocalDateTime = LocalDateTime.ofInstant(dt.toInstant, ZoneOffset.UTC)
}
