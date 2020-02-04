package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.ast.expressions.BooleanExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class WhereClause(
    querySession: QuerySession,
    parent: Option[TreeNode],
    booleanExpression: BooleanExpression) extends TreeNode {

  override def sql: String = s"WHERE ${booleanExpression.sql}"
}

object WhereClause {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): WhereClause = {

    val whereClause = WhereClause(querySession, parent, null)

    val booleanExpression = BooleanExpression(querySession, Some(whereClause))

    whereClause.copy(booleanExpression = booleanExpression)
  }
}
