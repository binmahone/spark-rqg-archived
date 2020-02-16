package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.{BooleanType, RQGConfig, RandomUtils}
import com.baidu.spark.rqg.ast.expressions.BooleanExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * whereClause
 *     : WHERE booleanExpression
 *     ;
 */
class WhereClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    val (min, max) = querySession.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
    querySession.allowedNestedExpressionCount = RandomUtils.choice(min, max)
    BooleanExpression(querySession, Some(this), BooleanType, isLast = true)
  }
  override def sql: String = s"WHERE ${booleanExpression.sql}"
}

/**
 * WhereClause generator
 */
object WhereClause extends TreeNodeGenerator[WhereClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): WhereClause = {

    new WhereClause(querySession, parent)
  }
}
