package carldata.hydra

import java.time.LocalDateTime
import java.util.Properties

import carldata.hs.Batch.BatchRecord
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob}
import carldata.hydra.Main.computationsDB
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

  val computationSet1: Seq[RealTimeJob] = Seq(
    AddRealTimeJob("calc1", code, Seq("c0"), "c-out", LocalDateTime.now, LocalDateTime.now.plusDays(5))
  )

  val inputSet1 = Seq(
    DataRecord("c0", LocalDateTime.now(), 1)
  )

  val batchInput = Seq(
    BatchRecord("6d696c6573", mphCode, Seq("kilometersPH"), "milesPH", LocalDateTime.now, LocalDateTime.now.plusDays(5))
  )

  val inputSet5 = Seq(
    DataRecord("c0", LocalDateTime.now.plusMinutes(1), 1),
    DataRecord("c0", LocalDateTime.now.plusMinutes(2), 1),
    DataRecord("c0", LocalDateTime.now.plusMinutes(3), 1),
    DataRecord("c0", LocalDateTime.now.plusMinutes(4), 1),
    DataRecord("c0", LocalDateTime.now.plusMinutes(5), 1)
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


  val dataProcessor = new DataProcessor(computationsDB)

  "StreamProcessing" should "not process event without computation" in {
    val input: Seq[(String, String)] = jsonStrData(inputSet5)

    val received = MockedStreams().config(buildConfig)
      .topology(builder => Main.buildDataStream(builder, "", dataProcessor))
      .input("data", strings, strings, input)
      .output[String, String]("data", strings, strings, 1)

    fromJson(received).size shouldEqual 0
  }

  it should "store computation in the DB" in {
    val input: Seq[(String, String)] = computationSet1.map(x => ("", x.toJson.compactPrint))
    val ts = inputSet1
      .groupBy(x => x.channelId)
      .map(x => (x._1, TimeSeries(x._2.map(y => y.timestamp).toVector, x._2.map(y => y.value).toVector)))

    val db = new TestCaseDB(ts)
    val rtCmdProcessor = new RTCommandProcessor(computationsDB, db)
    MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder, "", dataProcessor)
        Main.buildRealtimeStream(builder, "", rtCmdProcessor)
      }
      .input("hydra-rt", strings, strings, input)
      .output[String, String]("hydra-rt", strings, strings, input.size)

    Main.computationsDB.get("calc1").isEmpty shouldEqual false
  }

  it should "process events" in {
    val cmd: Seq[(String, String)] = computationSet1.map(x => ("", x.toJson.compactPrint))
    val input: Seq[(String, String)] = jsonStrData(inputSet5)
    val expected = Seq(DataRecord("c-out", inputSet5(0).timestamp, 2.0f)
      , DataRecord("c-out", inputSet5(1).timestamp, 2.0f)
      , DataRecord("c-out", inputSet5(2).timestamp, 2.0f)
      , DataRecord("c-out", inputSet5(3).timestamp, 2.0f)
      , DataRecord("c-out", inputSet5(4).timestamp, 2.0f))

    val ts = inputSet5
      .groupBy(x => x.channelId)
      .map(x => (x._1, TimeSeries(x._2.map(y => y.timestamp).toVector, x._2.map(y => y.value).toVector)))

    val db = new TestCaseDB(ts)
    val rtCmdProcessor = new RTCommandProcessor(computationsDB, db)
    val streams = MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder, "", dataProcessor)
        Main.buildRealtimeStream(builder, "", rtCmdProcessor)
      }
    streams.input("hydra-rt", strings, strings, cmd)
    //.output[String, String]("data", strings, strings, 6)

    val received = streams.input("data", strings, strings, input)
      .output[String, String]("data", strings, strings, 6)

    fromJson(received).filter(_.channelId == "c-out") shouldEqual expected
  }

}