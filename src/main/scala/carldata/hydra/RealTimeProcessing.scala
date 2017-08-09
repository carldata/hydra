package carldata.hydra

import carldata.hs.RealTime.RealTimeRecord
import carldata.sf.Interpreter
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}

import scala.collection.mutable

class RealTimeProcessing {
  val moduleMap: mutable.Map[String, Interpreter] = mutable.Map()
  val realTimeActions: mutable.Set[RealTimeRecord] = mutable.Set()

  val suplifier: ProcessorSupplier[String, String] = new ProcessorSupplier[String, String]() {
    override def get: Processor[String, String] = new Processor[String, String] {

      def init(context: ProcessorContext): Unit = {}

      def process(key: String, value: String): Unit = {}

      def punctuate(timestamp: Long): Unit = {}

      def close(): Unit = {
      }
    }
  }
}
