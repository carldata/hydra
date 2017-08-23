package carldata.hydra

import java.time.LocalDateTime

import carldata.series.TimeSeries
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DBConnectorTest extends FlatSpec with Matchers {
  "TestCaseDB" should "get last value " in {
    val m = Map[String, TimeSeries[Float]]("speed" -> TimeSeries.fromTimestamps(Seq(((1.0).toLong, 1), ((2.0).toLong, 3), ((3.0).toLong, 56)))
      , "power" -> TimeSeries.fromTimestamps(Seq(((1.0).toLong, 10000), ((2.0).toLong, 30000), ((3.0).toLong, 20000))))
    val db = new TestCaseDB(m)
    Await.result(db.getLastValue("speed"), Duration.Inf) shouldBe Some((LocalDateTime.of(1970, 1, 1, 0, 0, 3), 56.0))
  }

  it should "get series value " in {
    val m = Map[String, TimeSeries[Float]]("speed" -> TimeSeries.fromTimestamps(Seq(((1.0).toLong, 1), ((2.0).toLong, 3), ((3.0).toLong, 56)))
      , "power" -> TimeSeries.fromTimestamps(Seq(((1.0).toLong, 10000), ((2.0).toLong, 30000), ((3.0).toLong, 20000))))
    val db = new TestCaseDB(m)
    Await.result(db.getSeries ("speed", LocalDateTime.of(1970, 1, 1, 0, 0, 0), LocalDateTime.of(1970, 1, 1, 0, 0, 6)),
      Duration.Inf).values shouldBe TimeSeries.fromTimestamps(Seq(((1.0).toLong, 1), ((2.0).toLong, 3), ((3.0).toLong, 56))).values
  }
}
