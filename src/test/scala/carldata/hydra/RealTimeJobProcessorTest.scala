package carldata.hydra

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob}
import carldata.hydra.Main.jobsDB
import carldata.series.TimeSeries
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest._
import spray.json._


class RealTimeJobProcessorTest extends FlatSpec with Matchers {

  val now: LocalDateTime = LocalDateTime.now
  val code: String =
    """
      |def f(a: Number): Number = a + 1
      |def main(xs: TimeSeries): TimeSeries = map(xs, f)
    """.stripMargin

  val computationSet1: Seq[RealTimeJob] = Seq(
    AddRealTimeJob("calc1", code, Seq("c0"), "c-out", now, now.plusDays(5))
  )

  val inputSet1 = Seq(
    DataRecord("c0", now, 1)
  )


  val inputSet5 = Seq(
    DataRecord("c0", now.plusMinutes(1), 1),
    DataRecord("c0", now.plusMinutes(2), 1),
    DataRecord("c0", now.plusMinutes(3), 1),
    DataRecord("c0", now.plusMinutes(4), 1),
    DataRecord("c0", now.plusMinutes(5), 1)
  )

  val strings: Serde[String] = Serdes.String()


  "Batch Processor" should "store computation in the DB" in {
    val input: Seq[String] = computationSet1.map(_.toJson.compactPrint)
    val ts = inputSet1
      .groupBy(x => x.channelId)
      .map(x => (x._1, TimeSeries(x._2.map(y => y.timestamp).toVector, x._2.map(y => y.value).toVector)))

    val db = new TestCaseDB(ts)
    val rtProcessor = new RealTimeJobProcessor(jobsDB, db)
    input.foreach(rtProcessor.process)

    Main.jobsDB.get("calc1").nonEmpty shouldEqual true
  }

  it should "process events" in {
    val input: Seq[String] = computationSet1.map(_.toJson.compactPrint)
    val ts = inputSet5
      .groupBy(x => x.channelId)
      .map(x => (x._1, TimeSeries(x._2.map(y => y.timestamp).toVector, x._2.map(y => y.value).toVector)))

    val db = new TestCaseDB(ts)
    val rtJobProcessor = new RealTimeJobProcessor(jobsDB, db)
    val expected = Seq(
      DataRecord("c-out", inputSet5.head.timestamp, 2.0f),
      DataRecord("c-out", inputSet5(1).timestamp, 2.0f),
      DataRecord("c-out", inputSet5(2).timestamp, 2.0f),
      DataRecord("c-out", inputSet5(3).timestamp, 2.0f),
      DataRecord("c-out", inputSet5(4).timestamp, 2.0f)
    )

    val received = input.flatMap(rtJobProcessor.process).map(_.parseJson.convertTo[DataRecord])

    received shouldEqual expected
  }

}