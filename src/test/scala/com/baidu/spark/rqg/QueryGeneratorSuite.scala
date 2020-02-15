package com.baidu.spark.rqg

import com.baidu.spark.rqg.ast.{Column, Table}
import org.scalatest.FunSuite

class QueryGeneratorSuite extends FunSuite {

  test("basic test") {
    val columnInt = Column("rqg_table", "column_int", IntType)
    val columnString = Column("rqg_table", "column_string", StringType)
    val columnDecimal = Column("rqg_table", "column_decimal", DecimalType(10, 5))
    val table = Table("rqg_table", Array(columnInt, columnString, columnDecimal))
    println(new QueryGenerator(Array(table)).createQuery().sql)
  }
}
