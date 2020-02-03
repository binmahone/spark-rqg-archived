package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{BooleanType, DataType, RandomUtils}

case class Predicated(
    querySession: QuerySession,
    parent: Option[TreeNode],
    valueExpression: ValueExpression,
    predicateOption: Option[Predicate]) extends BooleanExpression {

  override def dataType: DataType[_] = if (predicateOption.isDefined) {
    BooleanType
  } else {
    valueExpression.dataType
  }

  override def sql: String = s"${valueExpression.sql} ${predicateOption.map(_.sql).getOrElse("")}"

  override def name: String =
    s"${valueExpression.name}${predicateOption.map("_" + _.name).getOrElse("")}"
}

object Predicated {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Predicated = {

    val predicated = Predicated(querySession, parent, null, null)

    val valueExpression = ValueExpression(querySession.copy(), Some(predicated))

    // val predicateOption = if (RandomUtils.nextBoolean()) {
    val predicateOption = if (false) {
      Some(Predicate(querySession.copy(), Some(predicated)))
    } else {
      None
    }

    predicated.copy(valueExpression = valueExpression, predicateOption = predicateOption)
  }
}
