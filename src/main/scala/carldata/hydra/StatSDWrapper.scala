package carldata.hydra

import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}
import org.slf4j.LoggerFactory

object StatSDWrapper {
  private val Log = LoggerFactory.getLogger(Main.getClass)
  private var client: Option[StatsDClient] = None

  def init(app: String, host: String): Unit = {
    try {
      client = Some(new NonBlockingStatsDClient(app, host, 8125))
    }
    catch {
      case e: Exception => Log.warn(e.getMessage)
    }
  }

  def increment(m: String): Unit = {
    client.foreach(_.incrementCounter(m))
  }

  def increment(m: String, i: Int): Unit = {
    client.foreach(sdc => for (x <- 1 to i) yield sdc.incrementCounter(m))
  }

  def gauge(m: String, i: Double): Unit = {
    client.foreach(_.recordGaugeValue(m, i))
  }
}
