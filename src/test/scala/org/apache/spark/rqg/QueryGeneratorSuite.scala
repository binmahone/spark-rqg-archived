package org.apache.spark.rqg

import org.apache.spark.rqg.ast.{Column, Table}
import org.scalatest.FunSuite

class QueryGeneratorSuite extends FunSuite {
  test("basic test") {
    RandomUtils.setSeed(1)
    val columnInt = Column("rqg_table", "column_int", IntType)
    val columnString = Column("rqg_table", "column_string", StringType)
    val columnDouble = Column("rqg_table", "column_double", DoubleType)
    val table = Table("rqg_table", Array(columnInt, columnString, columnDouble))
    new QueryGenerator(Array(table)).createQuery().sql
  }
}
