package com.baidu.spark.rqg

import com.baidu.spark.rqg.ast._
import org.scalatest.FunSuite

class ExpressionsSuite extends FunSuite {

  test("BooleanExpression") {

    val tables = Array(
      RQGTable("rqg_test_db","table_1",Array(RQGColumn("column_1",IntType), RQGColumn("column_2",BooleanType), RQGColumn("column_3",StringType(0,10)), RQGColumn("column_4",SmallIntType), RQGColumn("column_5",TinyIntType), RQGColumn("column_6",TinyIntType), RQGColumn("column_7",StringType(0,10)), RQGColumn("column_8",IntType))),
      RQGTable("rqg_test_db","table_2",Array(RQGColumn("column_1",FloatType), RQGColumn("column_2",StringType(0,10)), RQGColumn("column_3",BooleanType), RQGColumn("column_4",BooleanType))),
      RQGTable("rqg_test_db","table_3",Array(RQGColumn("column_1",TinyIntType), RQGColumn("column_2",TinyIntType), RQGColumn("column_3",BooleanType), RQGColumn("column_4",StringType(0,10)), RQGColumn("column_5",TinyIntType), RQGColumn("column_6",BigIntType), RQGColumn("column_7",BooleanType), RQGColumn("column_8",StringType(0,10)), RQGColumn("column_9",BigIntType))),
      RQGTable("rqg_test_db","table_4",Array(RQGColumn("column_1",BigIntType), RQGColumn("column_2",StringType(0,10)), RQGColumn("column_3",IntType), RQGColumn("column_4",TinyIntType), RQGColumn("column_5",DoubleType), RQGColumn("column_6",FloatType))),
      RQGTable("rqg_test_db","table_5",Array(RQGColumn("column_1",StringType(0,10)), RQGColumn("column_2",DecimalType(12,4)), RQGColumn("column_3",StringType(0,10)), RQGColumn("column_4",DecimalType(12,4)))))

    println(BooleanExpression(QuerySession(tables), Some(new SelectClause(QuerySession()))).toSql)
  }
  test("LogicalNot") {
    println(new LogicalNot(QuerySession(), None).toSql)
  }
  test("Predicated") {
    println(new Predicated(QuerySession(), None).toSql)
  }
  test("LogicalBinary") {
    println(new LogicalBinary(QuerySession(), None).toSql)
  }
  test("ValueExpression") {
    println(ValueExpression2(QuerySession(), None).toSql)
  }
  test("ArithmeticUnary") {
    println(new ArithmeticUnary(QuerySession(), None).toSql)
  }
  test("Comparison") {
    println(new Comparison(QuerySession(), None).toSql)
  }
  test("PrimaryExpression") {
    println(PrimaryExpression2(QuerySession(), None).toSql)
  }
  test("ConstantDefault") {
    val querySession = QuerySession(allowedDataTypes = Array(DecimalType(10, 5)))
    println(new ConstantDefault(querySession, None).toSql)
  }
  test("Star") {
    println(new Star(QuerySession(), None).toSql)
  }
  test("ColumnReference") {
    println(new ColumnReference(QuerySession(), None).toSql)
  }
  test("Predicate") {
    println(Predicate(QuerySession(), None).toSql)
  }
  test("BetweenPredicate") {
    println(new BetweenPredicate(QuerySession(), None).toSql)
  }
  test("InPredicate") {
    println(new InPredicate(QuerySession(), None).toSql)
  }
  test("LikePredicate") {
    println(new LikePredicate(QuerySession(), None).toSql)
  }
  test("NullPredicate") {
    println(new NullPredicate(QuerySession(), None).toSql)
  }
}
