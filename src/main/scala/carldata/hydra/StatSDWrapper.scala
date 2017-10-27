package carldata.hydra

import com.timgroup.statsd.StatsDClient

class StatSDWrapper(client: Option[StatsDClient]) {

  def increment(m: String): Unit = {
    client.foreach(_.incrementCounter(m))
  }

  def increment(m: String, i: Int): Unit = {
    client.foreach(sdc => for (x <- 1 to i) yield  sdc.incrementCounter(m))
  }

  def gauge(m: String, i: Double): Unit = {
    client.foreach(_.recordGaugeValue(m, i))
  }
}
