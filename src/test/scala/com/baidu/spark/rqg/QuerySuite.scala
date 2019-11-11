package com.baidu.spark.rqg

import org.scalatest.FunSuite

class QuerySuite extends FunSuite {
  test("basic test") {
    val col1 = Column("col-1", IntType)
    val table1 = Table("table-1", Array(col1))
    assert(!Query(
      SelectClause(Array.empty[SelectItem]),
      FromClause(table1, Array.empty[JoinClause])).isNestedQuery)
  }
}
