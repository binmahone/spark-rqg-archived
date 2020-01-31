package com.baidu.spark.rqg

import org.scalatest.FunSuite

class QueryGeneratorSuite extends FunSuite {

  test("basic test") {
    val columnInt = RQGColumn("column_int", IntType)
    val columnString = RQGColumn("column_string", StringType(0, 20))
    val columnDecimal = RQGColumn("column_decimal", DecimalType(10, 5))
    val table = RQGTable("rqg_db", "rqg_table", Seq(columnInt, columnString, columnDecimal))
    println(new QueryGenerator(Array(table)).createQuery().toSql)
  }
}
