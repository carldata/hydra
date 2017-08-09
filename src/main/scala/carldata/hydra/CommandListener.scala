package carldata.hydra

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction}
import carldata.sf.{Compiler, Core, Interpreter}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import spray.json.JsonParser

import scala.collection.mutable

object CommandListener {
  val moduleMap: mutable.Map[String, Interpreter] = mutable.Map()
  val realTimeActions: mutable.Set[RealTimeRecord] = mutable.Set()

  val suplifier: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {
    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {
        val rtr: RealTimeRecord = load(value)
        rtr.action match {
          case AddAction => {
            realTimeActions += rtr
            moduleMap ++ Map(rtr.calculation -> compile(rtr.script))
          }
          case RemoveAction => {
            realTimeActions -= rtr
            moduleMap -= rtr.calculation
          }
          case _ =>
        }
      }

      def punctuate(timestamp: Long): Unit = {}

      def close(): Unit = {}
    }
  }

  def compile(code: String): Option[Interpreter] = {
    Compiler.compile(code, Seq()).map { ast =>
      new Interpreter(ast, new Core())
    }.toOption
  }

  def load(j: String): RealTimeRecord = {
    JsonParser(j).convertTo[RealTimeRecord]
  }
}
