package com.baidu.spark.rqg

import org.scalatest.FunSuite

class QueryGeneratorSuite extends FunSuite {

  test("basic test") {

    val col1 = Column("col-1", IntType)
    val col2 = Column("col-2", IntType)
    val col3 = Column("col-3", IntType)
    val col4 = Column("col-4", IntType)
    val table1 = Table("table-1", Array(col1, col2))
    val table2 = Table("table-2", Array(col3, col4))

    val query = new QueryGenerator(new QueryProfile()).createQuery(Array(table1, table2))

    assert(query.fromClause.tableExpr.cols === Array(col1, col2))
    assert(query.fromClause.tableExpr.identifier === "t1")
    assert(query.fromClause.tableExpr.asInstanceOf[Table].name === "table-1")

    assert(query.selectClause.items.length == 1)
    assert(query.selectClause.items.head.valExpr === col1)
  }
}
