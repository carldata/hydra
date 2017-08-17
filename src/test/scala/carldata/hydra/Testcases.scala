package carldata.hydra

import java.io.File
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Properties

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord}
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsConfig
import org.scalatest._
import spray.json._

import scala.io.Source


class Testcases extends WordSpec with Matchers {

  case class ScriptTest(name: String, code: String, trigger: String, output: String, records: Seq[DataRecord], expected: Seq[DataRecord])

  "Testcases runner" should {
    "run all tests in folder: testcases" in {
      val xs = mkScriptTest(listFiles("testcases"))
      xs.count(_.isLeft) shouldEqual 0
      xs.filter(_.isLeft).foreach(println)

      xs.filter(_.isRight).foreach { x =>
        checkExecute(x.right.get)
      }
    }
  }

  def listFiles(folder: String): Seq[(String, String)] = {
    new File(folder)
      .listFiles
      .filter(x => x.isFile && x.getName.endsWith(".test"))
      .map { f => (f.getName, Source.fromFile(f).mkString) }
  }

  def mkScriptTest(s: Seq[(String, String)]): Seq[Either[String, Testcases.this.ScriptTest]] = {
    s.map { f =>
      groupToEither(f._1, getSection(f._2, "script"), getParams(f._2, "trigger"), getParams(f._2, "output"), getCsv(f._2, "records"), getCsv(f._2, "expected"))
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

  def checkExecute(s: ScriptTest): Unit = {
    val computationSet = Seq(
      RealTimeRecord(AddAction, s.trigger + s.output, s.code, s.trigger, s.output)
    )
    val strings: Serde[String] = Serdes.String()
    val cmd: Seq[(String, String)] = computationSet.map(x => ("", x.toJson.compactPrint))
    val input: Seq[(String, String)] = s.records.map(x => ("", x.toJson.compactPrint))

    val streams = MockedStreams().config(buildConfig)
      .topology { builder =>
        Main.buildDataStream(builder,"")
        Main.buildRealtimeStream(builder,"")
      }

    streams.input("hydra-rt", strings, strings, cmd).input("data", strings, strings, input)
      .output[String, String]("data", strings, strings, s.expected.size)
      .map(_._2.parseJson.convertTo[DataRecord])
      .filter(_.channel == s.output) shouldEqual s.expected
  }

  def groupToEither(name: String, code: Option[String], trigger: Option[String], output: Option[String], records: Option[Seq[DataRecord]], expected: Option[Seq[DataRecord]]): Either[String, ScriptTest] = {
    val st = for {
      c <- code
      t <- trigger
      o <- output
      ps <- records
      e <- expected
    } yield ScriptTest(name, c, t, o, ps, e)
    st.toRight(name)
  }

  def createDataRecord(x: Array[String]): DataRecord = {
    DataRecord(x(0), LocalDateTime.ofInstant(Instant.ofEpochMilli(x(1).toLong), ZoneId.systemDefault()), x(2).toFloat)
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
  def getParams(s: String, p: String): Option[String] = {
    getSection(s, "hydra-rt").get
      .split("\n")
      .find(_.contains(p))
      .map(_.split('=')(1))
  }

  /** Select single section from testcase */
  def getSection(s: String, c: String): Option[String] = {
    Some(s.split("```" + c)(1).split("```")(0)
      .replace("\r", System.lineSeparator)
      .replace("\n", System.lineSeparator))
  }
}