package carldata.hydra

import carldata.hydra.ComputationDB.Computation
import org.scalatest._


/** Tests for ComputationDB */
class ComputationDBTest extends FlatSpec with Matchers {

  "ComputationDB" should "add new computation" in {
    val db = new ComputationDB()
    val r = Computation("c1", "", "", "")
    db.add(r)
    db.get("c1") shouldBe Some(r)
  }

  it should "remove computation" in {
    val db = new ComputationDB()
    val r = Computation("c1", "", "", "")
    db.add(r)
    db.remove(r.id)
    db.get("c1") shouldBe None
  }

  it should "find computation by channel is" in {
    val db = new ComputationDB()
    val r = Computation("c1", "in1", "", "")
    db.add(r)
    db.findByChannel("in1") shouldBe List(r)
  }

}