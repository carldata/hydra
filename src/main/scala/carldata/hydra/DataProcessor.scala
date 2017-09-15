package carldata.hydra

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data._
import carldata.sf.Interpreter
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
    computationDB.findByChannel(input.channelId)
      .map(e => (e.destChannelId, execute(e.script, input.timestamp, input.value)))
      .map(x => x._2.map(y => DataRecord(x._1, input.timestamp, y)))
      .flatMap(_.toList)
      .map(serialize)
  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): DataRecord = {
    try {
      JsonParser(rec).convertTo[DataRecord]
    } catch {
      case _: ParsingException =>
        DataRecord("", LocalDateTime.now(), 0)
    }
  }

  /** Serialize message back to json */
  def serialize(rec: DataRecord): String = rec.toJson.compactPrint

  def execute(exec: Interpreter, ts: LocalDateTime, value: Float): Option[Float] = {

    exec.run("main", Seq(ts.toString, value)).right.toOption
      .flatMap {
        case v: Float => Some(v)
        case _ => None
      }

  }

}
