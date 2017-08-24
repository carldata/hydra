package carldata.hydra


import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.series.TimeSeries
import carldata.sf.Compiler.compile
import carldata.sf.Runtime.Value
import carldata.sf.core.TimeSeriesModule.TimeSeriesValue
import carldata.sf.{Interpreter, core}
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BatchProcessor {

  private val Log = LoggerFactory.getLogger("Hydra")

  def process(jsonStr: String, db: TimeSeriesDB): Seq[String] = {
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelId, outputChannelId, startDate, endDate)) => {
        val inputTs = Await.result(db.getSeries(inputChannelId, startDate, endDate), Duration.Inf)

        val result = compile(script, Seq(core.MathModule.header, core.DateTimeModule.header, core.TimeSeriesModule.header))
          .flatMap(exec => Interpreter(exec).run("main", Seq(TimeSeriesValue(inputTs))))
        val resultTs = getTimeSeries(result)
        val vs = resultTs.values
        val ids = resultTs.index

        ids.zip(vs)
          .map(x => DataRecord(outputChannelId, x._1, x._2))
          .map(serialize)
      }
      case _ => Seq()
    }

  }

  def getTimeSeries(s: Either[String, Value]): TimeSeries[Float] = {
    s.right.get.asInstanceOf[TimeSeriesValue].ts
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
