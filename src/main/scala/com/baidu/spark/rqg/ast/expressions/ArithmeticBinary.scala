package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.{DataType, RandomUtils}

case class ArithmeticBinary(
    querySession: QuerySession,
    parent: Option[TreeNode],
    dataType: DataType[_],
    operator: ArithmeticOperator,
    left: ValueExpression,
    right: ValueExpression) extends ValueExpression {

  override def name: String = s"${left.name}_${operator.name}_${right.name}"

  override def sql: String = s"(${left.sql}) ${operator.operator} (${right.sql})"
}

object ArithmeticBinary {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): ArithmeticBinary = {

    val arithmeticBinary = new ArithmeticBinary(querySession, parent, null, null, null, null)

    val operators = Array(
      ArithmeticOperator("PLUS", "+"),
      ArithmeticOperator("MINUS", "-"))

    val dataType = RandomUtils.choice(
      querySession.availableRelations.flatMap(_.dataTypes).distinct)

    val operator = RandomUtils.choice(operators)

    val left = ValueExpression(querySession.copy(), parent)
    val right = ValueExpression(querySession.copy(), parent)

    arithmeticBinary.copy(dataType = dataType, left = left, right = right, operator = operator)
  }
}

case class ArithmeticOperator(name: String, operator: String)
