package carldata.hydra


import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.EventBus._
import carldata.sf.Compiler.compile
import carldata.sf.core.TimeSeriesModule.TimeSeriesValue
import carldata.sf.{Interpreter, core}
import com.outworkers.phantom.dsl.ContactPoints
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BatchProcessor {

  private val Log = LoggerFactory.getLogger("Hydra")

  def process(jsonStr: String, dbName: String, keyspace: String): Unit = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelId, outputChannelId, startDate, endDate)) => {
        //TODO: load data from cassandra
        val db = new CassandraDB(ContactPoints(Seq(dbName)).keySpace(keyspace))
        val ts = Await.result(db.data.getSeries(inputChannelId, startDate, endDate), Duration.Inf)

        //TODO: Send event to event bus about starting batch job

        //TODO: compile script
        val result = compile(script, Seq(core.MathModule.header, core.DateTimeModule.header, core.TimeSeriesModule.header))
          .flatMap(exec => Interpreter(exec).run("main", Seq(TimeSeriesValue(ts))))

        //TODO: Execute script with the loaded time series

        //TODO: Save returned time series to the output channel

        //TODO: Send event to event bus that batch job has ended.

      }

      case _ =>
    }

  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): Option[BatchRecord] = {
    try {
      Some(JsonParser(rec).convertTo[BatchRecord])
    } catch {
      case _: ParsingException => None
    }
  }

  /** Convert EventBus to json */
  def serialize(rec: EventBusRecord): String = {
    ""
  }
}
