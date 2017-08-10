package carldata.hydra

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction, UnknownAction}
import carldata.sf.{Compiler, Core, Interpreter}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.mutable

object Listener {

  val realTimeActions: mutable.Set[RealTimeRecord] = mutable.Set()
  val moduleMap: mutable.Map[String, Interpreter] = mutable.Map()

  val command: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {
    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {
        val rtr: RealTimeRecord = load(value)
        rtr.action match {
          case AddAction => {
            val interpreter = compile(rtr.script.replace("\n", System.lineSeparator()))
            interpreter match {
              case None => println("fail")
              case _ => {
                realTimeActions += rtr
                moduleMap ++ Map(rtr.calculation -> compile(rtr.script))
              }
            }

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

    def load(j: String): RealTimeRecord = {
      try JsonParser(j).convertTo[RealTimeRecord]
      catch {
        case e: ParsingException => {
          return new RealTimeRecord(UnknownAction, "", "", "", "")
        }
      }
    }
  }

  val data: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {

    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {
        val entity: DataRecord = load(value)
        realTimeActions.filter(x => x.trigger.eq(entity.channel))
          .map(x => RealTimeProcessing.run(entity, x, moduleMap.get(x.calculation).get))
      }

      def punctuate(timestamp: Long): Unit = {}

      def close(): Unit = {
      }
    }

    def load(j: String): DataRecord = {
      try JsonParser(j).convertTo[DataRecord]
      catch {
        case e: ParsingException => {
          return new DataRecord("", LocalDateTime.now(), 0)
        }
      }

    }
  }

  def compile(code: String): Option[Interpreter] = {
    Compiler.compile(code, Seq()).map { ast =>
      new Interpreter(ast, new Core())
    }.toOption
  }


}
