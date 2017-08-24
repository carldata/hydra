package carldata.hydra

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

import carldata.series.TimeSeries
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DBConnectorTest extends FlatSpec with Matchers {

  "TestCaseDB" should "get series value " in {
    val m = Map[String, TimeSeries[Float]]("speed" -> TimeSeries.fromTimestamps(Seq((1l, 1), (2l, 3), (3l, 56)))
      , "power" -> TimeSeries.fromTimestamps(Seq((1l, 10000), (2l, 30000), (3l, 20000))))
    val db = new TestCaseDB(m)
    Await.result(db.getSeries("speed", LocalDateTime.of(1970, 1, 1, 0, 0, 0), LocalDateTime.of(1970, 1, 1, 0, 0, 6)),
      Duration.apply(30, TimeUnit.SECONDS)).values shouldBe TimeSeries.fromTimestamps(Seq((1l, 1), (2l, 3), (3l, 56))).values
  }
}
