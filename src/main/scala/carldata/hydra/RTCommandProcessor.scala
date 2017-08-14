package carldata.hydra

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction}
import carldata.hydra.ComputationDB.Computation
import carldata.sf.Compiler.compile
import carldata.sf.{Core, Interpreter}
import spray.json.JsonParser.ParsingException
import spray.json._

/**
  * Data processing pipeline
  */
class RTCommandProcessor(computationDB: ComputationDB) {

  /**
    * Process data event. Single event can generate 0 or more then 1 computed events.
    * The number of output events depends on how many computations are defined on given channel
    */
  def process(jsonStr: String): Unit = {
    println(jsonStr)
    deserialize(jsonStr) match {
      case Some(RealTimeRecord(AddAction, calculationId, script, trigger, outputChannel)) => {
        val execCode = compile(script, Seq(Core.header))
          .map { ast => new Interpreter(ast, new Core()) }
          .right.get
        val comp = Computation(calculationId, trigger, execCode, outputChannel)
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
