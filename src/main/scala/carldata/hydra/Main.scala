package carldata.hydra

import java.net.InetAddress
import java.util.Properties

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Main application.
  *
  * Connects to the Kafka topics and process events.
  */
object Main {

  private val Log = LoggerFactory.getLogger(Main.getClass)

  /** How long to wait for new batch of data. In milliseconds */
  val POLL_TIMEOUT = 100
  /** Maximum number of records read by poll function */
  val MAX_POLL_RECORDS = 100
  /** Data topic name */
  val DATA_TOPIC = "data"
  /** Real time topic name */
  val REAL_TIME_TOPIC = "hydra-rt"

  /** Memory db with computation which should be triggered by data topic */
  val jobsDB = new ComputationDB()

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

  /** Kafka consumer (reader) configuration */
  def consumerConfig(params: Params): Properties = {
    val strDeserializer = (new StringDeserializer).getClass.getName
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaBroker)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, params.prefix + "hydra")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props
  }

  /** Kafka producer (writer) configuration */
  def producerConfig(brokers: String): Properties = {
    val strSerializer = (new StringSerializer).getClass.getName
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, strSerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, strSerializer)
    props
  }


  /** Init connection to the database */
  def initCassandra(params: Params): CassandraDB = {
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


  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    Log.info("Hydra started ")
    StatsD.init("hydra", params.statsDHost)
    val db = initCassandra(params)
    Log.info("Application started")
    run(params, db)
    Log.info("Application Stopped")
  }

  /**
    * Main processing loop:
    *  - Read batch of data from all topics
    *  - Split by channel. And for each channel
    *    - Create db statement
    *    - Execute statement on db
    */
  def run(params: Params, cassandraDB: CassandraDB): Unit = {
    val rtProcessor = new RealTimeJobProcessor(jobsDB, cassandraDB)
    val dataProcessor = new DataProcessor(jobsDB)
    val kafkaConfig = consumerConfig(params)
    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    val producer = new KafkaProducer[String, String](producerConfig(params.kafkaBroker))

    consumer.subscribe(List(DATA_TOPIC, REAL_TIME_TOPIC).map(t => params.prefix + t).asJava)

    while (true) {
      try {
        val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
        val records = getTopicMessages(batch, params.prefix + DATA_TOPIC)
        val xs1 = records.flatMap(dataProcessor.process)
        val realTimeMessages = getTopicMessages(batch, params.prefix + REAL_TIME_TOPIC)
        val xs2 = realTimeMessages.flatMap(rtProcessor.process)

        (xs1 ++ xs2).foreach(r => sendToDataTopic(producer)("", r))
        consumer.commitSync()
        StatsD.increment("data.out.count", records.size)
      }
      catch {
        case e: Exception =>
          StatsD.increment("data.error")
          Log.error(e.toString)
      }
    }
    consumer.close()
  }

  def sendToDataTopic(producer: KafkaProducer[String, String])(key: String, value: String): Unit = {
    val data = new ProducerRecord[String, String](DATA_TOPIC, key, value)
    producer.send(data)
  }


  /** Extract only messages (skip keys) for given topic */
  def getTopicMessages(batch: ConsumerRecords[String, String], topic: String): Seq[String] =
    batch.records(topic).asScala.map(_.value()).toList

}


