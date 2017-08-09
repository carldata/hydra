package carldata.hydra

import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.processor.StateStoreSupplier


import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import carldata.sf.Interpreter
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction}

import scala.collection.JavaConverters.asJavaIterableConverter
import java.util.logging.Logger

import org.apache.kafka.streams.processor.TopologyBuilder

import scala.collection.mutable

/**
  * Main application.
  *
  * Connects to the Kafka topics and process events.
  */
object Main {
  val moduleMap: mutable.Map[String, Interpreter] = mutable.Map()
  val realTimeActions: mutable.Set[RealTimeRecord] = mutable.Set()
  private val logger = Logger.getLogger("Hydra")

  case class Params(kafkaBroker: String)

  def buildConfig(params: Params): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "hydra")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaBroker)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  def parseArgs(args: Array[String]): Params = {
    Params("localhost:9092")
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    logger.info("Hydra started: " + params)
    val config = buildConfig(params)
    val builder: KStreamBuilder = new KStreamBuilder()
    val dataStream: KStream[String, String] = builder.stream("DataIn")
    dataStream.to("DataOut")

    val listener = Listener
    val clBuilder : TopologyBuilder = new TopologyBuilder()
    clBuilder.addSource("CL_source","hydra_rt")
      .addProcessor("CL_process",listener.command, "CL_source")

    val dlBuilder : TopologyBuilder = new TopologyBuilder()
    dlBuilder.addSource("DL_source","data")
      .addProcessor("DL_process",listener.data,"DL_source")


    val streams: KafkaStreams = new KafkaStreams(builder, config)
    streams.start()
    val streams2: KafkaStreams = new KafkaStreams(clBuilder, config)
    streams2.start()
    val streams3: KafkaStreams = new KafkaStreams(dlBuilder, config)
    streams3.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
      streams2.close(10, TimeUnit.SECONDS)
      streams3.close(10, TimeUnit.SECONDS)
      logger.info("Hydra stopped")
    }))
  }
}


