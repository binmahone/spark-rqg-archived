package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.BooleanType

case class JoinCriteria(
    querySession: QuerySession,
    parent: Option[TreeNode],
    valueExpression: ValueExpression) extends TreeNode {

  override def sql: String = s"ON ${valueExpression.sql}"
}

object JoinCriteria {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinCriteria = {

    val joinCriteria = JoinCriteria(querySession, parent, null)

    val valueExpression = ValueExpression(
      querySession.copy(allowedDataTypes = Array(BooleanType)), Some(joinCriteria))

    joinCriteria.copy(valueExpression = valueExpression)
  }
}
