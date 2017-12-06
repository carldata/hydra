package carldata.hydra

import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

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

import scala.io.Source


class Testcases extends WordSpec with Matchers {

  trait ProcessType

  case object RealTimeProcess extends ProcessType

  case object UnknownProcess extends ProcessType

  case class TestCaseFile(name: String, text: String, processType: ProcessType)

  case class ScriptRTTest(name: String, code: String, trigger: Seq[String], output: String, records: Seq[DataRecord], expected: Seq[DataRecord])


  val dataProcessor = new DataProcessor(computationsDB)

  "Testcases runner" should {


    "run all tests in folder: testcases" in {
      val filesList: Seq[TestCaseFile] = listFiles("testcases")
      filesList.foreach {
        x =>
          x.processType match {
            case RealTimeProcess =>
              val xs = mkScriptRTTest(filesList.filter(_.processType == RealTimeProcess))
              xs.count(_.isLeft) shouldEqual 0
              //xs.filter(_.isLeft).foreach(println)
              xs.filter(_.isRight).foreach { x =>
                checkExecuteRT(x.right.get)
              }
            case UnknownProcess => "Testcase:" + x.name + " do not have hydra-rtparams section"
          }
      }
    }
  }

  def listFiles(folder: String): Seq[TestCaseFile] = {
    new File(folder)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith(".test"))
      .map { f => (f.getName, Source.fromFile(f).getLines().mkString("\n")) }
      .map { x => {
        if (x._2.contains("hydra-rt")) TestCaseFile(x._1, x._2, RealTimeProcess)
        else TestCaseFile("", "", UnknownProcess)
      }
      }
  }

  def mkScriptRTTest(s: Seq[TestCaseFile]): Seq[Either[String, Testcases.this.ScriptRTTest]] = {
    s.map { f =>
      groupToRTEither(f.name, getSection(f.text, "script"), getInputs(f.text, "trigger", "hydra-rt"), getParams(f.text, "output", "hydra-rt"), getCsv(f.text, "records"), getCsv(f.text, "expected"))
    }
  }


  def buildConfig: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "hydra")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p
  }

  def checkExecuteRT(s: ScriptRTTest): Unit = {
    val computationSet: Seq[RealTimeJob] = Seq(
      AddRealTimeJob(s.trigger + s.output, s.code, s.trigger, s.output, s.records.head.timestamp, s.records.last.timestamp.plusMinutes(1))
    )

    val ts = s.records
      .groupBy(x => x.channelId)
      .map(x => (x._1, TimeSeries(x._2.map(y => y.timestamp).toVector, x._2.map(y => y.value).toVector)))

    val db = new TestCaseDB(ts)
    val strings: Serde[String] = Serdes.String()
    val cmd: Seq[(String, String)] = computationSet.map(x => ("", x.toJson.compactPrint))
    val input: Seq[(String, String)] = s.records.map(x => ("", x.toJson.compactPrint))
    val rtCmdProcessor = new RTCommandProcessor(computationsDB, db)
    val streams = MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder, "", dataProcessor)
        Main.buildRealtimeStream(builder, "", rtCmdProcessor)
      }

    val received = streams.input("hydra-rt", strings, strings, cmd)
      .output[String, String]("data", strings, strings, s.expected.size)


    received.map(_._2.parseJson.convertTo[DataRecord])
      .filter(_.channelId == s.output) shouldEqual s.expected
  }

  def groupToRTEither(name: String, code: Option[String], trigger: Option[Seq[String]], output: Option[String], records: Option[Seq[DataRecord]], expected: Option[Seq[DataRecord]]): Either[String, ScriptRTTest] = {
    val st = for {
      c <- code
      t <- trigger
      o <- output
      ps <- records
      e <- expected
    } yield ScriptRTTest(name, c, t, o, ps, e)
    st.toRight(name)
  }

  def createDataRecord(x: Array[String]): DataRecord = {
    DataRecord(x(0), LocalDateTime.parse(x(1), DateTimeFormatter.ISO_LOCAL_DATE_TIME), x(2).toFloat)
  }

  /** For 'records' and 'expected' sections */
  def getCsv(s: String, p: String): Option[Seq[DataRecord]] = {
    val csv = getSection(s, p).get
      .split("\n")
      .filter(_.contains(','))
    if (csv.isEmpty) return Some(Seq(DataRecord("", LocalDateTime.now, 0)))
    Some {
      csv.map(x => x.split(","))
        .map(createDataRecord)
    }
  }

  /** From 'hydra-rt' section get param by name */
  def getParams(code: String, param: String, name: String): Option[String] = {
    getSection(code, name).get
      .split("\n")
      .find(_.contains(param))
      .map(_.split('=')(1))
  }

  def getInputs(code: String, param: String, name: String): Option[Seq[String]] = {
    getParams(code, param, name)
      .map(x => x.split(","))
  }

  /** Select single section from testcase */
  def getSection(code: String, section: String): Option[String] = {
    Some(code.split("```" + section)(1).split("```")(0))
  }
}