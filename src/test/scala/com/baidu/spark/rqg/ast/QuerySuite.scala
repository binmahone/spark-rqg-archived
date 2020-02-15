package com.baidu.spark.rqg.ast

import org.scalatest.FunSuite

import com.baidu.spark.rqg._

class QuerySuite extends FunSuite {

  // simulates a set of tables from database
  private val tables = Array(
    Table("table_1", Array(
      Column("table_1", "column_1",IntType), Column("table_1", "column_2",BooleanType),
      Column("table_1", "column_3",StringType), Column("table_1", "column_4",SmallIntType),
      Column("table_1", "column_5",TinyIntType), Column("table_1", "column_6",TinyIntType),
      Column("table_1", "column_7",StringType), Column("table_1", "column_8",IntType))),
    Table("table_2", Array(
      Column("table_2", "column_1",FloatType), Column("table_2", "column_2",StringType),
      Column("table_2", "column_3",BooleanType), Column("table_2", "column_4",BooleanType))),
    Table("table_3", Array(
      Column("table_3", "column_1",TinyIntType), Column("table_3", "column_2",TinyIntType),
      Column("table_3", "column_3",BooleanType), Column("table_3", "column_4",StringType),
      Column("table_3", "column_5",TinyIntType), Column("table_3", "column_6",BigIntType),
      Column("table_3", "column_7",BooleanType), Column("table_3", "column_8",StringType),
      Column("table_3", "column_9",BigIntType))),
    Table("table_4", Array(
      Column("table_4", "column_1",BigIntType), Column("table_4", "column_2",StringType),
      Column("table_4", "column_3",IntType), Column("table_4", "column_4",TinyIntType),
      Column("table_4", "column_5",DoubleType), Column("table_4", "column_6",FloatType))),
    Table("table_5", Array(
      Column("table_5", "column_1",StringType), Column("table_5", "column_2",DecimalType(12,4)),
      Column("table_5", "column_3",StringType), Column("table_5", "column_4",DecimalType(12,4)),
      Column("table_5", "column_5",IntType))))

  // Use function rather than val in order to generate new querySession every time
  private def querySession = QuerySession(tables)

  private val queryValidator = new QueryValidator(querySession)

  test("Query") {
    for (_ <- 0 until 1000) {
      queryValidator.assertValid(Query(querySession, None).sql)
    }
  }
}
