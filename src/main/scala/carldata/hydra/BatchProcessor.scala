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


class BatchProcessor() {

  private val Log = LoggerFactory.getLogger(this.getClass.getName)

  def process(jsonStr: String, db: TimeSeriesDB): Seq[String] = {
    val startTime = System.currentTimeMillis()
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelIds, outputChannelId, startDate, endDate)) =>
        Log.info(s"Run batch on channels $inputChannelIds, with range $startDate - $endDate")
        StatsD.increment("batch.count")
        val futures = inputChannelIds.map(id => db.getSeries(id, startDate, endDate))
        val inputTs = Await.result(Future.sequence(futures), 30.seconds)
        StatsD.increment("batch.in.count", inputTs.map(_.length).sum)
        make(script).flatMap(exec => Interpreter(exec, db).run("main", inputTs)) match {
          case Right(xs) =>
            val endTime = System.currentTimeMillis()
            val resultTs = xs.asInstanceOf[TimeSeries[Float]]
            StatsD.increment("batch.out.count", resultTs.length)
            StatsD.gauge("batch.rate", 1000.0 * resultTs.length / (endTime-startTime))
            resultTs.dataPoints
              .map(x => DataRecord(outputChannelId, x._1, x._2))
              .map(serialize)
          case _ => Seq()
        }

      case _ => Seq()
    }

  }

  /** Convert from json with exception handling */
  def deserialize(jsonStr: String): Option[BatchRecord] = {
    try {
      Some(JsonParser(jsonStr).convertTo[BatchRecord])
    } catch {
      case _: ParsingException =>
        Log.warn("Error deserializing batch job:\n" + jsonStr)
        StatsD.increment("batch.errors.parser")
        None
    }
  }


  /** Serialize message back to json */
  def serialize(rec: DataRecord): String = rec.toJson.compactPrint
}
