package carldata.hydra

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.AddRealTimeJob
import carldata.series.TimeSeries
import carldata.sf.Compiler.make
import carldata.sf.Interpreter
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class BatchProcessor() {

  private val Log = LoggerFactory.getLogger(this.getClass.getName)

  def process(job: AddRealTimeJob, db: DBImplementation): Seq[String] = {
    val startTime = System.currentTimeMillis()

        StatsD.increment("batch")
        val futures = job.inputChannelIds.map(id => db.getSeries(id, job.startDate, job.endDate))
        val inputTs = Await.result(Future.sequence(futures), 30.seconds)
        StatsD.increment("batch.in.records", inputTs.map(_.length).sum)
        make(job.script).flatMap(exec => Interpreter(exec, db).run("main", inputTs)) match {
          case Right(xs) =>
            val endTime = System.currentTimeMillis()
            val resultTs = xs.asInstanceOf[TimeSeries[Float]]
            StatsD.increment("batch.out.records", resultTs.length)
            StatsD.gauge("batch.rate", 1000.0 * resultTs.length / (endTime-startTime))
            resultTs.dataPoints
              .map(x => DataRecord(job.outputChannelId, x._1, x._2))
              .map(serialize)
          case Left(err) =>
            StatsD.increment("batch.errors.script")
            Log.warn("Can't compile script:" + job.script)
            Log.warn(err)
            Seq()
        }


  }


  /** Serialize message back to json */
  def serialize(rec: DataRecord): String = rec.toJson.compactPrint
}
