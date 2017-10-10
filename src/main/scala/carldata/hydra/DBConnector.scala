package carldata.hydra

import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import carldata.series.TimeSeries
import carldata.sf.core.DBImplementation
import com.outworkers.phantom.dsl._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class DataEntity(channel: String, timestamp: String, value: Float)

case class LookupEntity(id: String, x: Float, y: Float)

trait TimeSeriesDB extends DBImplementation {
  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] //Future[List[DataEntity]]

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

  object channel extends StringColumn with PartitionKey

  object timestamp extends StringColumn with PrimaryKey with ClusteringOrder with Descending

  object value extends FloatColumn

  def getById(id: String): Future[Option[DataEntity]] = {
    select.where(_.channel eqs id).one()
  }

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    select.where(_.channel eqs name)
      .and(_.timestamp gte convert(from))
      .and(_.timestamp lte convert(to))
      .fetch
      .map(x => new TimeSeries(x.map { y => {
        (dateParse(y.timestamp), y.value)
      }
      }))
  }

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = {
    select.where(_.channel eqs name)
      .orderBy(_.timestamp desc)
      .one() map {
      x => x.map(c => (dateParse(c.timestamp), c.value))
    }
  }

  def dateParse(str: String): LocalDateTime = {
    val formatter = new DateTimeFormatterBuilder()
      .parseCaseInsensitive
      .appendValue(ChronoField.YEAR)
      .appendLiteral('-')
      .appendValue(ChronoField.MONTH_OF_YEAR)
      .appendLiteral('-')
      .appendValue(ChronoField.DAY_OF_MONTH)
      .optionalStart.appendLiteral(' ').optionalEnd
      .optionalStart.appendLiteral('T').optionalEnd
      .optionalStart
      .appendValue(ChronoField.HOUR_OF_DAY)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR)
      .optionalStart.appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE).optionalEnd
      .optionalEnd
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter

    LocalDateTime.parse(str, formatter)
  }

  def convert(time: LocalDateTime): String = {
    time.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }
}

class CassandraDB(val keyspace: CassandraConnection) extends Database[CassandraDB](keyspace) with TimeSeriesDB {

  object data extends Data with keyspace.Connector

  object lookup_table extends Lookup_Table with keyspace.Connector

  override def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = data.getSeries(name, from, to)

  def getTable(id: String): IndexedSeq[(Float, Float)] = {
    Await.result(lookup_table.getTable(id), 30.seconds).map(x => (x.x, x.y)).toIndexedSeq
  }

}

class TestCaseDB(ts: Map[String, TimeSeries[Float]]) extends TimeSeriesDB {

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    Future(ts.get(name).get.slice(from, to))
  }

  val lookup_table = Seq(("velocity", 1f, 100f)
    , ("velocity", 3f, 200f)
    , ("velocity", 5f, 400f))

  def getTable(id: String): IndexedSeq[(Float, Float)] = {
    lookup_table.filter(p => p._1.equals(id)).map(x => (x._2, x._3)).toIndexedSeq
  }


}


