package carldata.hydra

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import spray.json.JsonParser.ParsingException
import spray.json._

/**
  * Data processing pipeline
  */
class DataProcessor(computationDB: ComputationDB) {

  /**
    * Process data event. Single event can generate 0 or more then 1 computed events.
    * The number of output events depends on how many computations are defined on given channel
    */
  def process(jsonStr: String): Seq[String] = {
    val input = deserialize(jsonStr)
    computationDB.findByChannel(input.channel)
      .map(c => serialize(DataRecord(c.destChannelId, input.ts, input.value)))
  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): DataRecord = {
    try{
      JsonParser(rec).convertTo[DataRecord]
    } catch {
      case _: ParsingException =>
        DataRecord("", LocalDateTime.now(), 0)
    }
  }

  /** Serialize message back to json */
  def serialize(rec: DataRecord): String = rec.toJson.compactPrint

}
