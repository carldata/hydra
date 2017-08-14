package carldata.hydra

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, _}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Main application.
  *
  * Connects to the Kafka topics and process events.
  */
object Main {

  private val Log = LoggerFactory.getLogger("Hydra")

  /** Memory db with computation which should be triggered by data topic */
  val computationsDB = new ComputationDB()
  val rtCmdProcessor = new RTCommandProcessor(computationsDB)
  val dataProcessor = new DataProcessor(computationsDB)

  case class Params(kafkaBroker: String)

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    Params(kafka)
  }

  def buildConfig(params: Params): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "hydra")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaBroker)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    Log.info("Hydra started: " + params)
    val config = buildConfig(params)
    // Build processing topology
    val builder: KStreamBuilder = new KStreamBuilder()
    buildRealtimeStream(builder)
    buildDataStream(builder)

    // Start topology
    val streams = new KafkaStreams(builder, config)
    streams.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
      Log.info("Hydra stopped")
    }))
  }

  /** Data topic processing pipeline */
  def buildDataStream(builder: KStreamBuilder): Unit = {
    val ds: KStream[String, String] = builder.stream("data")
    val dsOut: KStream[String, String] = ds.flatMapValues( x => dataProcessor.process(x).asJava)
    dsOut.to("data")
  }

  /** Data topic processing pipeline */
  def buildRealtimeStream(builder: KStreamBuilder): Unit = {
    val cs: KStream[String, String] = builder.stream("hydra-rt")
    val _: KStream[String, Unit] = cs.mapValues(x => rtCmdProcessor.process(x))
  }
}


