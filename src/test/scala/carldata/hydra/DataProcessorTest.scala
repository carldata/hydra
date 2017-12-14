package carldata.hydra

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob}
import carldata.hydra.Main.jobsDB
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest._
import spray.json._


class DataProcessorTest extends FlatSpec with Matchers {

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


  "Data Processing" should "not process event without computation" in {
    val dataProcessor = new DataProcessor(jobsDB)
    val input: Seq[String] = inputSet5.map(_.toJson.compactPrint)

    val received = input.flatMap(dataProcessor.process)

    received.size shouldEqual 0
  }

}