package carldata.hydra


import java.time.LocalDateTime

import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.EventBus._
import com.outworkers.phantom.dsl.ContactPoints
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

class BatchProcessor {

  private val Log = LoggerFactory.getLogger("Hydra")

  def process(jsonStr: String, dbName: String, keyspace: String): Unit = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelId, outputChannelId, startDate, endDate)) => {
        //TODO: load data from cassandra
        val db = new CassandraDB(ContactPoints(Seq(dbName)).keySpace(keyspace))
        db.dataTable.getSeries("",LocalDateTime.of(2013,3,18,9,40,0),LocalDateTime.of(2013,3,18,19,40,0))

        //TODO: Send event to event bus about starting batch job


        //TODO: compile script

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
