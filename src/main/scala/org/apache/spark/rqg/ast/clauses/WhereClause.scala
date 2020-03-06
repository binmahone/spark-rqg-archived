package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{BooleanType, RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.expressions.BooleanExpression
import org.apache.spark.rqg.ast.{AggPreference, QueryContext, TreeNode, TreeNodeGenerator}

/**
 * whereClause
 *     : WHERE booleanExpression
 *     ;
 */
class WhereClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    val (min, max) = queryContext.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
    queryContext.allowedNestedExpressionCount = RandomUtils.choice(min, max)
    queryContext.aggPreference = AggPreference.FORBID
    BooleanExpression(queryContext, Some(this), BooleanType, isLast = true)
  }
  override def sql: String = s"WHERE ${booleanExpression.sql}"
}

/**
 * WhereClause generator
 */
object WhereClause extends TreeNodeGenerator[WhereClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): WhereClause = {

    new WhereClause(querySession, parent)
  }
}
