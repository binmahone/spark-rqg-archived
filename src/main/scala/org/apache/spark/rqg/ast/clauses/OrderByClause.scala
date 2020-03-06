package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * Not implement yet
 */
class OrderByClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = ""
}

object OrderByClause extends TreeNodeGenerator[OrderByClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): OrderByClause = {

    new OrderByClause(querySession, parent)
  }
}
