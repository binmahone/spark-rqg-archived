package com.baidu.spark.rqg

import org.scalatest.FunSuite

class DataTypeSuite extends FunSuite {
  test("basic test") {
    DataType.TYPES.foreach { x =>
      assert(DataType.nameToType(x.typeName) == x)
    }
  }

}
