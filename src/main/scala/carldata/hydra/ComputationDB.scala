package carldata.hydra

import scala.collection.mutable

/**
  * This memory db contains information about computations which should be run in RealTime mode.
  * This is a mutable data structure.
  * This db should allow to add several different computation on the same source channel
  */
object ComputationDB{

  case class Computation(id: String, srcChannelId: String, script: String, destChannelId: String)
}


class ComputationDB {
  import carldata.hydra.ComputationDB._

  private val computations = mutable.Map[String, Computation]()

  /** Add computation to the database */
  def add(rec: Computation): Unit = {
    computations += (rec.id -> rec)
  }

  /** Remove computation from the database */
  def remove(computationId: String): Unit = {
    computations -= computationId
  }

  /** Return computation by its id */
  def get(computationId: String): Option[Computation] = computations.get(computationId)

  /** Find computation by its source channel */
  def findByChannel(channelId: String): Seq[Computation] =
    computations.values.filter(_.srcChannelId == channelId).toList
}
