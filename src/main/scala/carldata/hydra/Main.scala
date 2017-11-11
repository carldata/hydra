package carldata.hydra

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.PlainTextAuthProvider
import com.outworkers.phantom.connectors.ContactPoints
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
  val rtCmdProcessor = new RTCommandProcessor(computationsDB)
  val dataProcessor = new DataProcessor(computationsDB)
  val batchProcessor = new BatchProcessor()

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
    val statsDHost = stringArg(args, "statsDHost", "none")

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

    // Build processing topology
    val builder: KStreamBuilder = new KStreamBuilder()
    buildRealtimeStream(builder, params.prefix, db, rtCmdProcessor)
    buildBatchStream(builder, params.prefix, db, batchProcessor)
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
  def buildRealtimeStream(builder: KStreamBuilder, prefix: String = "", db: TimeSeriesDB, rtCmdProcessor: RTCommandProcessor): Unit = {
    val cs: KStream[String, String] = builder.stream(prefix + "hydra-rt")
    cs.foreach((_, v) => rtCmdProcessor.process(v, db))
  }

  /** Batch processing pipeline */
  def buildBatchStream(builder: KStreamBuilder, prefix: String, db: TimeSeriesDB, batchProcessor: BatchProcessor): Unit = {

    val ds: KStream[String, String] = builder.stream(prefix + "hydra-batch")
    val dsOut: KStream[String, String] = ds.flatMapValues(v => batchProcessor.process(v, db).asJava)
    dsOut.to(prefix + "data")
  }

  def initDB(params: Params): CassandraDB = {
    if (params.user == "" || params.pass == "") {
      new CassandraDB(ContactPoints(params.db).keySpace(params.keyspace))
    } else {
      new CassandraDB(ContactPoints(params.db)
        .withClusterBuilder(_.withAuthProvider(new PlainTextAuthProvider(params.user, params.pass)))
        .keySpace(params.keyspace))
    }
  }

}


