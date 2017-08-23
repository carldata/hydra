package carldata.hydra

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction}
import carldata.hydra.ComputationDB.Computation
import carldata.sf.Compiler.compile
import carldata.sf.{Interpreter, core}
import org.slf4j.LoggerFactory
import spray.json.JsonParser.ParsingException
import spray.json._

/**
  * Data processing pipeline
  */
class RTCommandProcessor(computationDB: ComputationDB) {

  private val Log = LoggerFactory.getLogger("Hydra")

  /**
    * Process data event. Single event can generate 0 or more then 1 computed events.
    * The number of output events depends on how many computations are defined on given channel
    */
  def process(jsonStr: String): Unit = {
    Log.info(jsonStr)
    deserialize(jsonStr) match {
      case Some(RealTimeRecord(AddAction, calculationId, script, trigger, outputChannel)) =>
        compile(script, Seq(core.MathModule.header))
          .map { ast => new Interpreter(ast, Seq(new core.MathModule())) }
          .foreach { exec =>
            val comp = Computation(calculationId, trigger, exec, outputChannel)
            computationDB.add(comp)
          }

      case Some(RealTimeRecord(RemoveAction, calculationId, _, _, _)) =>
        computationDB.remove(calculationId)
      case _ =>
    }

  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): Option[RealTimeRecord] = {
    try {
      Some(JsonParser(rec).convertTo[RealTimeRecord])
    } catch {
      case _: ParsingException => None
    }
  }

}
