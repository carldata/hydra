package carldata.hydra


import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.series.TimeSeries
import carldata.sf.Compiler.make
import carldata.sf.Interpreter
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class BatchProcessor {

  private val Log = LoggerFactory.getLogger("Hydra")

  def process(jsonStr: String, db: TimeSeriesDB): Seq[String] = {
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelIds, outputChannelId, startDate, endDate)) => {
        val futures = inputChannelIds.map(id => db.getSeries(id, startDate, endDate))
        val inputTs = Await.result(Future.sequence(futures), 30.seconds)
        make(script).flatMap(exec => Interpreter(exec, db).run("main", inputTs)) match {
          case Right(xs) =>
            val resultTs = xs.asInstanceOf[TimeSeries[Float]]
            val vs = resultTs.values
            val ids = resultTs.index
            ids.zip(vs)
              .map(x => DataRecord(outputChannelId, x._1, x._2))
              .map(serialize)
          case _ => Seq()
        }

      }
      case _ => Seq()
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


  /** Serialize message back to json */
  def serialize(rec: DataRecord): String = rec.toJson.compactPrint
}
