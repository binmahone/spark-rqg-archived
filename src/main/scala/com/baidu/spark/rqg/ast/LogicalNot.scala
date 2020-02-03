package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{BooleanType, DataType}

case class LogicalNot(
    querySession: QuerySession,
    parent: Option[TreeNode],
    booleanExpression: BooleanExpression) extends BooleanExpression {

  override def dataType: DataType[_] = BooleanType

  override def name: String = s"not_${booleanExpression.name}"

  override def sql: String = s"NOT ${booleanExpression.sql}"
}

object LogicalNot {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): LogicalNot = {

    val logicalNot = LogicalNot(querySession, parent, null)

    val booleanExpression = BooleanExpression(
      querySession.copy(allowedDataTypes = Array(BooleanType)), Some(logicalNot))

    logicalNot.copy(booleanExpression = booleanExpression)
  }
}
