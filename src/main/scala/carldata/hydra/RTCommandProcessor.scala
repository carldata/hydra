package carldata.hydra

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeJobRecord, RemoveAction}
import carldata.hydra.ComputationDB.Computation
import carldata.sf.Compiler.make
import carldata.sf.Interpreter
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

/**
  * Data processing pipeline
  */
class RTCommandProcessor(computationDB: ComputationDB) {

  private val Log = LoggerFactory.getLogger(this.getClass)

  /**
    * Process data event. Single event can generate 0 or more then 1 computed events.
    * The number of output events depends on how many computations are defined on given channel
    */
  def process(jsonStr: String, db: TimeSeriesDB): Unit = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(RealTimeJobRecord(AddAction, calculationId, script, trigger, outputChannel)) =>
        make(script)
          .map { ast => Interpreter(ast, db) }
          .foreach { exec =>
            trigger.foreach { t =>
              val comp = Computation(calculationId, t, exec, outputChannel)
              computationDB.add(comp)
            }
          }

      case Some(RealTimeJobRecord(RemoveAction, calculationId, _, _, _)) =>
        computationDB.remove(calculationId)
      case _ =>
    }

  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): Option[RealTimeJobRecord] = {
    try {
      Some(JsonParser(rec).convertTo[RealTimeJobRecord])
    } catch {
      case _: ParsingException => None
    }
  }

}
