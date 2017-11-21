package carldata.hydra

import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob, RemoveRealTimeJob}
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hydra.ComputationDB.Computation
import carldata.sf.Compiler.make
import carldata.sf.Interpreter
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

/**
  * Data processing pipeline
  */
class RTCommandProcessor(computationDB: ComputationDB, db: DBConnector) {

  private val Log = LoggerFactory.getLogger(this.getClass)

  /**
    * Process data event. Single event can generate 0 or more then 1 computed events.
    * The number of output events depends on how many computations are defined on given channel
    */
  def process(jsonStr: String): Seq[String] = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(AddRealTimeJob(calculationId, script, trigger, outputChannel, startDate, endDate)) =>
        StatsD.increment("rt.count")
        make(script)
          .map { ast => Interpreter(ast, db.getImplementation) }
          .foreach { exec =>
            trigger.foreach { t =>
              StatsD.increment("rt.out.count")
              val comp = Computation(calculationId, t, exec, outputChannel)
              computationDB.add(comp)
            }
          }

        BatchProcessor.process(AddRealTimeJob(calculationId, script, trigger, outputChannel, startDate, endDate), db)

      case Some(RemoveRealTimeJob(calculationId)) =>
        computationDB.remove(calculationId)
        Seq()
      case _ => Seq()
    }

  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): Option[RealTimeJob] = {
    try {
      Some(JsonParser(rec).convertTo[RealTimeJob])
    } catch {
      case _: ParsingException =>
        StatsD.increment("rt.errors.parser")
        None
    }
  }

}
