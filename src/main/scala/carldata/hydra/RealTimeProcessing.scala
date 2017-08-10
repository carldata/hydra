package carldata.hydra

import carldata.hs.Data.DataRecord
import carldata.hs.RealTime.RealTimeRecord
import carldata.sf.Interpreter
import carldata.sf.Runtime.NumberValue

object RealTimeProcessing {

  def run(d: DataRecord, rt: RealTimeRecord, i: Interpreter): Unit = {
    i.run(rt.calculation, Seq(NumberValue(d.value)))
    //TODO: save to db
  }
}
