package org.apache.spark.rqg

import org.scalatest.FunSuite

class DataTypesSuite extends FunSuite {
  test("BooleanType") {
    assert(BooleanType.sameType(BooleanType))
    assert(!BooleanType.sameType(IntType))
  }

  test("IntType") {
    assert(IntType.sameType(IntType))
    assert(!IntType.sameType(TinyIntType))
  }

  test("DecimalType") {
    assert(DecimalType(20, 3).sameType(DecimalType(20, 3)))
    assert(!DecimalType(8, 3).sameType(DecimalType(20, 3)))
  }
}
