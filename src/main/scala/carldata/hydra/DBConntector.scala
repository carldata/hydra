package carldata.hydra

//import carldata.series.TimeSeries
import com.outworkers.phantom.dsl._
import com.outworkers.phantom.builder.query.{CreateQuery, ExecutableQuery}
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
//import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import java.time.format.DateTimeFormatterBuilder

case class DataEntity(channel: String, timestamp: String, value: Float)

trait TimeSeriesDB {
  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[List[DataEntity]] // Future[TimeSeries[Float]]

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]]
}

abstract class Data extends Table[Data, DataEntity] with TimeSeriesDB {

  object channel extends StringColumn with PartitionKey

  object timestamp extends StringColumn with PrimaryKey with ClusteringOrder with Descending

  object value extends FloatColumn

  def getById(id: String): Future[Option[DataEntity]] = {
    select.where(_.channel eqs id).one()
  }

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[List[DataEntity]] = { // Future[TimeSeries[Float]] = {
    //TODO
    select.where(_.channel eqs name)
      .and(_.timestamp gte convert(from))
      .and(_.timestamp lte convert(to))
      .fetch



    /*TimeSeries.fromTimestamps(x.map { y => {
      (unixTimestamp(y.time), y.value)
    }
    }.toSeq)
*/

  }

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = {
    println(name + "\t <=last!")
    select.where(_.channel eqs name)
      .orderBy(_.timestamp desc)
      .one() map {
      x => x.map(c => (dateParse(c.timestamp), c.value))
    }
  }

  def convert(time: String): LocalDateTime = {
    //LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimestamp(time) * 1000), ZoneId.systemDefault)
    LocalDateTime.now
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

class CassandraDB(val keyspace: CassandraConnection) extends Database[CassandraDB](keyspace) {

  object data extends Data with keyspace.Connector

}

/*
//TODO
class TestCaseDB(ts: Map[String, TimeSeries[Float]]) extends TimeSeriesDB {

  def getSeries(name: String, from: LocalDateTime, to: LocalDateTime): Future[TimeSeries[Float]] = {
    Future(ts.get(name).get.slice(from, to))
  }

  def getLastValue(name: String): Future[Option[(LocalDateTime, Float)]] = {
    Future(ts.get(name).map(xs => (xs.index.last, xs.values.last)))
  }
}
*/

