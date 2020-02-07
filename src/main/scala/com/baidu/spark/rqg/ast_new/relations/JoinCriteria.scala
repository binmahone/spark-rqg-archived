package com.baidu.spark.rqg.ast_new.relations

import com.baidu.spark.rqg.{BooleanType, DataType}
import com.baidu.spark.rqg.ast_new.expressions.BooleanExpression
import com.baidu.spark.rqg.ast_new.{QuerySession, TreeNode, TreeNodeGenerator}

class JoinCriteria(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  require(querySession.joiningRelation.isDefined, "no relation to join during creating JoinCriteria")

  querySession.requiredRelationalExpressionCount = 1
  querySession.allowedNestedExpressionCount = 5
  querySession.allowedDataTypes = DataType.joinableDataTypes

  val booleanExpression = BooleanExpression(querySession, Some(this), BooleanType, isLast = true)
  override def sql: String = s"ON ${booleanExpression.sql}"
}

object JoinCriteria extends TreeNodeGenerator[JoinCriteria] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinCriteria = {
    new JoinCriteria(querySession, parent)
  }
}
