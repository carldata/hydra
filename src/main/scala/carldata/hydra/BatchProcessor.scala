package carldata.hydra


import carldata.hs.Batch.BatchRecordJsonProtocol._
import carldata.hs.Batch._
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.sf.Compiler.compile
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
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(BatchRecord(calculationId, script, inputChannelId, outputChannelId, startDate, endDate)) => {
        //load data from cassandra
       // val db = new CassandraDB(ContactPoints(Seq(dbName)).keySpace(keyspace))
        val ts = Await.result(db.getSeries(inputChannelId, startDate, endDate), Duration.Inf)

        //TODO: Send event to event bus about starting batch job

        //compile script
        //Execute script with the loaded time series
        val result = compile(script, Seq(core.MathModule.header, core.DateTimeModule.header, core.TimeSeriesModule.header))
          .flatMap(exec => Interpreter(exec).run("main", Seq(TimeSeriesValue(ts))))

        //Save returned time series to the output channel
        val vs = result.right.get.asInstanceOf[TimeSeriesValue].ts.values
        val ids = result.right.get.asInstanceOf[TimeSeriesValue].ts.index

        ids.zip(vs)
          .map(x => DataRecord(outputChannelId, x._1, x._2))
          .map(serialize)

        //TODO: Send event to event bus that batch job has ended.

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
