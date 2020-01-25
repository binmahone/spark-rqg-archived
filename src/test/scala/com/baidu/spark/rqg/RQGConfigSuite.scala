package com.baidu.spark.rqg

import org.scalatest.FunSuite
import com.baidu.spark.rqg.RQGConfig._

class RQGConfigSuite extends FunSuite {

  test("basic") {
    val configDefault = RQGConfig.load("conf/rqg-defaults.conf")
    assert(configDefault.getBound(TABLE_COUNT) == (1, 2))
    assert(configDefault.getProbability(FROM) == 1.0)
    assert(configDefault.getFlag(ONLY_SELECT_ITEM))
    assert(configDefault.getWeight(JOIN).toSet ==
      Set(WeightEntry("INNER", 0.7d), WeightEntry("LEFT", 0.2d),
        WeightEntry("RIGHT", 0.05d), WeightEntry("FULL_OUTER", 0.04d),
        WeightEntry("CROSS", 0.01d)))

    // Default Value
    val configEmpty = RQGConfig.load("conf/rqg-empty.conf")
    assert(configEmpty.getBound(TABLE_COUNT) == TABLE_COUNT.defaultValue)
    assert(configEmpty.getProbability(FROM) == FROM.defaultValue)
    assert(configEmpty.getFlag(ONLY_SELECT_ITEM) == ONLY_SELECT_ITEM.defaultValue)
    assert(configEmpty.getWeight(JOIN).toSet ==
      JOIN.defaultValue.asInstanceOf[List[WeightEntry]].toSet)
  }
}
