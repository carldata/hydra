package carldata.hydra

import java.time.LocalDateTime

import carldata.series.TimeSeries
import carldata.sf.core.DBImplementation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TestCaseDB(ts: Map[String, TimeSeries[Float]]) extends DBConnector {

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    Future(ts(name).slice(from, to))
  }

  val lookup_table = Seq(("velocity", 1f, 100f), ("velocity", 3f, 200f), ("velocity", 5f, 400f))

  def getImplementation: DBImplementation = (id: String) => {
    lookup_table.filter(p => p._1.equals(id)).map(x => (x._2, x._3)).toIndexedSeq
  }
}
