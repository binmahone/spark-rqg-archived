package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.BooleanType

case class JoinCriteria(
    querySession: QuerySession,
    parent: Option[TreeNode],
    booleanExpression: BooleanExpression) extends TreeNode {

  override def sql: String = s"ON ${booleanExpression.sql}"
}

object JoinCriteria {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinCriteria = {

    val joinCriteria = JoinCriteria(querySession, parent, null)

    val booleanExpression = BooleanExpression(
      querySession.copy(allowedDataTypes = Array(BooleanType)), Some(joinCriteria))

    joinCriteria.copy(booleanExpression = booleanExpression)
  }
}
