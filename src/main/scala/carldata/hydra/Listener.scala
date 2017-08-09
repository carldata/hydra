package carldata.hydra

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeRecord, RemoveAction}
import carldata.sf.{Compiler, Core, Interpreter}
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}
import org.apache.kafka.streams.state.KeyValueStore
import spray.json.JsonParser

import scala.collection.mutable

object Listener {
  //val moduleMap: mutable.Map[String, Interpreter] = mutable.Map()
  val moduleStore: KeyValueStore[String, Interpreter] = null
  val realTimeActions: mutable.Set[RealTimeRecord] = mutable.Set()

  val command: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {
    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {
        val rtr: RealTimeRecord = load(value)
        println(key +"....."+value )
        println(rtr.calculation)
        rtr.action match {
          case AddAction => {
            realTimeActions += rtr
            //        moduleMap ++ Map(rtr.calculation -> compile(rtr.script))
            moduleStore.put(rtr.calculation, compile(rtr.script).get)
          }
          case RemoveAction => {
            realTimeActions -= rtr
            //      moduleMap -= rtr.calculation
            moduleStore.delete(rtr.calculation)
          }
          case _ =>
        }
      }

      def punctuate(timestamp: Long): Unit = {}

      def close(): Unit = {}
    }

    def load(j: String): RealTimeRecord = {
      JsonParser(j).convertTo[RealTimeRecord]
    }
  }

  val data: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {

    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {
        println(key +"....."+value )
        val entity: DataRecord = load(value)
        println(entity.channel)
        realTimeActions.filter(x => x.trigger.eq(entity.channel))
          .map(x => RealTimeProcessing.run(entity, x, moduleStore.get(x.calculation)))
      }

      def punctuate(timestamp: Long): Unit = {}

      def close(): Unit = {
      }
    }

    def load(j: String): DataRecord = {
      JsonParser(j).convertTo[DataRecord]
    }
  }


  def compile(code: String): Option[Interpreter] = {
    Compiler.compile(code, Seq()).map { ast =>
      new Interpreter(ast, new Core())
    }.toOption
  }


}
