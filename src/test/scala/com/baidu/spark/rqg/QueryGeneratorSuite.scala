package com.baidu.spark.rqg

import org.scalatest.FunSuite

class QueryGeneratorSuite extends FunSuite {

  test("basic test") {
    assert(new QueryGenerator().createQuery() == "SELECT * FROM table")
  }
}
