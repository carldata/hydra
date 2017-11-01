package carldata.hydra

import java.time._

import carldata.series.TimeSeries
import carldata.sf.core.DBImplementation
import com.outworkers.phantom.dsl._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class DataEntity(channel: String, timestamp: DateTime, value: Float)

case class LookupEntity(id: String, x: Float, y: Float)

trait TimeSeriesDB extends DBImplementation {
  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]]

}

abstract class Lookup_Table extends Table[Lookup_Table, LookupEntity] {

  object id extends StringColumn with PartitionKey

  object x extends FloatColumn

  object y extends FloatColumn

  def getTable(id: String): Future[List[LookupEntity]] = {
    select.where(_.id eqs id).fetch
  }
}

abstract class Data extends Table[Data, DataEntity] {

  private val Log = LoggerFactory.getLogger(getClass)

  object channel extends StringColumn with PartitionKey

  object timestamp extends DateTimeColumn with PrimaryKey with ClusteringOrder with Descending

  object value extends FloatColumn

  def getById(id: String): Future[Option[DataEntity]] = {
    select.where(_.channel eqs id).one()
  }

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    select.where(_.channel eqs name)
      .and(_.timestamp gte toDateTime(from))
      .and(_.timestamp lte toDateTime(to))
      .fetch
      .map(x => new TimeSeries(x.map { y => {
        (fromDateTime(y.timestamp), y.value)
      }
      }))
  }

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = {
    select.where(_.channel eqs name)
      .orderBy(_.timestamp.desc)
      .one() map {
      x => x.map(c => (fromDateTime(c.timestamp), c.value))
    }
  }

  def fromDateTime(dt: DateTime): LocalDateTime =
    LocalDateTime.ofInstant(Instant.ofEpochMilli(dt.toInstant.getMillis), ZoneOffset.UTC)

  def toDateTime(dt: LocalDateTime): DateTime = new DateTime(dt.toInstant(ZoneOffset.UTC).toEpochMilli)

}

class CassandraDB(val keyspace: CassandraConnection) extends Database[CassandraDB](keyspace) with TimeSeriesDB {

  object data extends Data with keyspace.Connector

  object lookup_table extends Lookup_Table with keyspace.Connector

  override def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] =
    data.getSeries(name, from, to)

  def getTable(id: String): IndexedSeq[(Float, Float)] = {
    Await.result(lookup_table.getTable(id), 30.seconds).map(x => (x.x, x.y)).toIndexedSeq
  }

}

class TestCaseDB(ts: Map[String, TimeSeries[Float]]) extends TimeSeriesDB {

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    Future(ts(name).slice(from, to))
  }

  val lookup_table = Seq(("velocity", 1f, 100f), ("velocity", 3f, 200f), ("velocity", 5f, 400f))

  def getTable(id: String): IndexedSeq[(Float, Float)] = {
    lookup_table.filter(p => p._1.equals(id)).map(x => (x._2, x._3)).toIndexedSeq
  }


}


