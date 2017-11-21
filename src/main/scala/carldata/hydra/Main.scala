package carldata.hydra

import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KafkaStreams, _}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Main application.
  *
  * Connects to the Kafka topics and process events.
  */
object Main {

  private val Log = LoggerFactory.getLogger(Main.getClass)

  /** Memory db with computation which should be triggered by data topic */
  val computationsDB = new ComputationDB()
  val dataProcessor = new DataProcessor(computationsDB)

  case class Params(kafkaBroker: String, prefix: String, db: Seq[String], keyspace: String, user: String,
                    pass: String, statsDHost: String)

  def stringArg(args: Array[String], key: String, default: String): String = {
    val name = "--" + key + "="
    args.find(_.contains(name)).map(_.substring(name.length)).getOrElse(default).trim
  }

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = stringArg(args, "kafka", "localhost:9092")
    val prefix = stringArg(args, "prefix", "")
    val db = stringArg(args, "db", "localhost").split(",")
    val user = stringArg(args, "user", "")
    val pass = stringArg(args, "pass", "")
    val keyspace = stringArg(args, "keyspace", "")
    val statsDHost = stringArg(args, "statsd-host", "none")

    Params(kafka, prefix, db, keyspace, user, pass, statsDHost)
  }

  def buildConfig(params: Params): Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, params.prefix + "hydra")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaBroker)
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, (new WallclockTimestampExtractor).getClass.getName)
    p
  }


  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    Log.info("Hydra started ")
    StatsD.init("hydra", params.statsDHost)
    val config = buildConfig(params)
    val db = initDB(params)
    val rtCmdProcessor = new RTCommandProcessor(computationsDB, db)
    // Build processing topology
    val builder: KStreamBuilder = new KStreamBuilder()
    buildRealtimeStream(builder, params.prefix, rtCmdProcessor)
    buildDataStream(builder, params.prefix, dataProcessor)


    // Start topology
    val streams = new KafkaStreams(builder, config)
    streams.setUncaughtExceptionHandler((t: Thread, e: Throwable) => {
      Log.error("Uncaught exception\n" + e.getMessage)
    })
    streams.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close(10, TimeUnit.SECONDS)
      Log.info("Hydra stopped")
    }))
  }

  /** Data topic processing pipeline */
  def buildDataStream(builder: KStreamBuilder, prefix: String = "", dataProcessor: DataProcessor): Unit = {
    val ds: KStream[String, String] = builder.stream(prefix + "data")
    val dsOut: KStream[String, String] = ds.flatMapValues(x => dataProcessor.process(x).asJava)
    dsOut.to(prefix + "data")
  }

  /** Data topic processing pipeline */
  def buildRealtimeStream(builder: KStreamBuilder, prefix: String = "", rtCmdProcessor: RTCommandProcessor): Unit = {
    val cs: KStream[String, String] = builder.stream(prefix + "hydra-rt")
    val dsOut: KStream[String, String] = cs.flatMapValues(v => rtCmdProcessor.process(v).asJava)
    dsOut.to(prefix + "data")

  }

  /** Init connection to the database */
  def initDB(params: Params): CassandraDB = {
    val builder = Cluster.builder()
      .withSocketOptions(new SocketOptions().setReadTimeoutMillis(30000))
      .addContactPoints(params.db.map(InetAddress.getByName).asJava)
      .withPort(9042)

    if (params.user != "" && params.pass != "") {
      builder.withCredentials(params.user, params.pass)
    }

    val session = builder.build().connect(params.keyspace)
    CassandraDB(session)
  }

}


