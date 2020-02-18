package org.apache.spark.rqg

import org.scalatest.FunSuite

class DataTypesSuite extends FunSuite {

  test("BooleanType") {
    assert(BooleanType.acceptsType(BooleanType))
    assert(BooleanType.sameType(BooleanType))
    assert(!BooleanType.acceptsType(IntType))
  }

  test("IntType") {
    assert(IntType.acceptsType(TinyIntType))
    assert(IntType.sameType(IntType))
    assert(!IntType.sameType(TinyIntType))
    assert(!IntType.acceptsType(DoubleType))
  }

  test("DecimalType") {
    assert(DecimalType().sameType(DecimalType(20, 3)))
    assert(DecimalType().acceptsType(DecimalType(20, 3)))
    assert(DecimalType().acceptsType(DoubleType))
    assert(DecimalType().acceptsType(IntType))
    assert(!DecimalType().acceptsType(BooleanType))
  }
}
