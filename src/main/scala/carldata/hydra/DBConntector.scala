package carldata.hydra

import java.time._
import java.time.temporal.ChronoField

import carldata.series.TimeSeries
import com.outworkers.phantom.dsl._

import java.time.format.DateTimeFormatterBuilder

import scala.concurrent.Future

case class DataEntity(channel: String, timestamp: String, value: Float)

trait TimeSeriesDB {
  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] //Future[List[DataEntity]]

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]]
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
    println(name + "\t <=last!")
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
    time.toString
  }
}

class CassandraDB(val keyspace: CassandraConnection) extends Database[CassandraDB](keyspace) with TimeSeriesDB {

  object data extends Data with keyspace.Connector

  override def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = data.getSeries(name, from, to)

  override def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = data.getLastValue(name)
}

class TestCaseDB(ts: Map[String, TimeSeries[Float]]) extends TimeSeriesDB {

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    Future(ts.get(name).get.slice(from, to))
  }

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = {
    Future(ts.get(name).map(xs => (xs.index.last, xs.values.last)))
  }
}


