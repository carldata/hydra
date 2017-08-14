package carldata.hydra

import carldata.hydra.ComputationDB.Computation
import org.scalatest._
import carldata.sf.Compiler
import carldata.sf.{Core, Interpreter}

/** Tests for ComputationDB */
class ComputationDBTest extends FlatSpec with Matchers {
  val code =
    """
      |module Test1
      |
      |def main(dt: DateTime, a: Number): Number = a
    """.stripMargin
  val interpreter = Compiler.compile(code, Seq(Core.header))
    .map { ast => new Interpreter(ast, new Core()) }
    .right.get

  "ComputationDB" should "add new computation" in {

    val db = new ComputationDB()
    val r = Computation("c1", "", interpreter, "")
    db.add(r)
    db.get("c1") shouldBe Some(r)
  }

  it should "remove computation" in {
    val db = new ComputationDB()
    val r = Computation("c1", "", interpreter, "")
    db.add(r)
    db.remove(r.id)
    db.get("c1") shouldBe None
  }

  it should "find computation by channel is" in {
    val db = new ComputationDB()
    val r = Computation("c1", "in1", interpreter, "")
    db.add(r)
    db.findByChannel("in1") shouldBe List(r)
  }

}