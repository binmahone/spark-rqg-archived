package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.expressions.BooleanExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class AggregationClause(
    querySession: QuerySession,
    parent: Option[TreeNode],
    groupingExpressions: Array[BooleanExpression]) extends TreeNode {

  override def sql: String = s"GROUP BY ${groupingExpressions.map(_.sql).mkString(", ")}"
}

object AggregationClause {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): AggregationClause = {

    val aggregationClause = AggregationClause(querySession, parent, null)

    val expressions = (0 until RandomUtils.choice(1, 2)).map { _ =>
      BooleanExpression(querySession.copy(), Some(aggregationClause))
    }.toArray

    aggregationClause.copy(groupingExpressions = expressions)
  }
}
