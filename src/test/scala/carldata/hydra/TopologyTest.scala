package carldata.hydra

import java.time.LocalDateTime
import java.util.Properties

import carldata.hs.Batch.BatchRecord
import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord}
import carldata.series.TimeSeries
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsConfig
import org.scalatest._
import spray.json._


object TopologyTest {

  val code: String =
    """
      |def main(dt: DateTime, a: Number): Number = a + 1
    """.stripMargin

  val mphCode: String =
    """
      |def f(a: Number): Number = 1.6093 * a
      |
      |def main(xs: TimeSeries): TimeSeries = map(xs, f)
    """.stripMargin

  val computationSet1 = Seq(
    RealTimeRecord(AddAction, "calc1", code, "c3", "c-out")
  )

  val inputSet1 = Seq(
    DataRecord("c0", LocalDateTime.now(), 1)
  )

  val batchInput = Seq(
    BatchRecord("6d696c6573", mphCode, "kilometersPH", "milesPH", LocalDateTime.now, LocalDateTime.now.plusDays(5))
  )

  val inputSet5 = Seq(
    DataRecord("c1", LocalDateTime.now(), 1),
    DataRecord("c2", LocalDateTime.now(), 1),
    DataRecord("c3", LocalDateTime.now(), 1),
    DataRecord("c4", LocalDateTime.now(), 1),
    DataRecord("c5", LocalDateTime.now(), 1)
  )

  def buildConfig: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "hydra")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p
  }

  val strings: Serde[String] = Serdes.String()

  def jsonStrData(data: Seq[DataRecord]): Seq[(String, String)] = data.map(x => ("", x.toJson.compactPrint))

  def fromJson(data: Seq[(String, String)]): Seq[DataRecord] = data.map(_._2.parseJson.convertTo[DataRecord])

}


/** Tests streams processing topology */
class TopologyTest extends FlatSpec with Matchers {

  import TopologyTest._

  "StreamProcessing" should "not process event without computation" in {
    val input: Seq[(String, String)] = jsonStrData(inputSet5)

    val received = MockedStreams().config(buildConfig)
      .topology(builder => Main.buildDataStream(builder))
      .input("data", strings, strings, input)
      .output[String, String]("data", strings, strings, 1)

    fromJson(received).size shouldEqual 0
  }

  it should "store computation in the DB" in {
    val input: Seq[(String, String)] = computationSet1.map(x => ("", x.toJson.compactPrint))

    MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder)
        Main.buildRealtimeStream(builder)
      }
      .input("hydra-rt", strings, strings, input)
      .output[String, String]("hydra-rt", strings, strings, input.size)

    Main.computationsDB.get("calc1").isEmpty shouldEqual false
  }

  it should "process events" in {
    val cmd: Seq[(String, String)] = computationSet1.map(x => ("", x.toJson.compactPrint))
    val input: Seq[(String, String)] = jsonStrData(inputSet5)
    val expected = Seq(DataRecord("c-out", inputSet5(2).timestamp, 2.0f))

    val streams = MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder)
        Main.buildRealtimeStream(builder)
      }
    streams.input("hydra-rt", strings, strings, cmd)

    val received = streams.input("data", strings, strings, input)
      .output[String, String]("data", strings, strings, 2)

    fromJson(received).filter(_.channelId == "c-out") shouldEqual expected
  }

  "BatchProcessing" should "process data" in {
    val time = LocalDateTime.now
    val input: Seq[(String, String)] = batchInput.map(x => ("", x.toJson.compactPrint))
    val expected = Seq(DataRecord("milesPH", time.plusHours(1), 1.6093f), DataRecord("milesPH", time.plusHours(2), 3.2186f), DataRecord("milesPH", time.plusHours(3), 4.8279f))
    val m = Map[String, TimeSeries[Float]]("kilometersPH" -> TimeSeries.fromColumns(Seq(time.plusHours(1), time.plusHours(2), time.plusHours(3)), Seq(1, 2, 3))
      , "kilometersPH2" -> TimeSeries.fromColumns(Seq(time, time.plusMinutes(1), time.plusMinutes(2)), Seq(11, 22, 33)))
    val db = new TestCaseDB(m)

    val received = MockedStreams().config(buildConfig)
      .topology(builder => Main.buildBatchStream(builder, "", db))
      .input("hydra-batch", strings, strings, input)
      .output[String, String]("data", strings, strings, expected.size)

    fromJson(received).filter(_.channelId == "milesPH").toList shouldBe expected
  }

}