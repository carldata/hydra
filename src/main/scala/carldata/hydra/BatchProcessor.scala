package carldata.hydra


import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.EventBus._
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

class BatchProcessor {

  private val Log = LoggerFactory.getLogger("Hydra")


  def process(jsonStr: String): Unit = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelId, outputChannelId, startDate, endDate)) => {
        //TODO: load data from cassandra

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
