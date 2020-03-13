package org.apache.spark.rqg

import org.scalatest.FunSuite
import org.apache.spark.rqg.RQGConfig._

class RQGConfigSuite extends FunSuite {

  test("basic") {
    val configDefault = RQGConfig.load("rqg-defaults.conf")
    assert(configDefault.getBound(JOIN_COUNT) == (0, 2))
    assert(configDefault.getProbability(FROM) == 1.0)
    assert(configDefault.getWeight(JOIN_TYPE).toSet ==
      Set(WeightEntry("INNER", 0.7d), WeightEntry("LEFT", 0.2d),
        WeightEntry("RIGHT", 0.05d), WeightEntry("FULL_OUTER", 0.04d),
        WeightEntry("CROSS", 0.01d)))

    // Default Value
    val configEmpty = RQGConfig.load("rqg-empty.conf")
    assert(configEmpty.getBound(JOIN_COUNT) == JOIN_COUNT.defaultValue)
    assert(configEmpty.getProbability(FROM) == FROM.defaultValue)
    assert(configEmpty.getWeight(JOIN_TYPE).toSet ==
      JOIN_TYPE.defaultValue.asInstanceOf[List[WeightEntry]].toSet)
  }
}
