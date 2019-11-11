package com.baidu.spark.rqg

import org.scalatest.FunSuite

class SqlWriterSuite extends FunSuite {
  test("basic test") {
    assert(new SqlWriter().writeQuery(null) == "SELECT * FROM table")
  }
}
