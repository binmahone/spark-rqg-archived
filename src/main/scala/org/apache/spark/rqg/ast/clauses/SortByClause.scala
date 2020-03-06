package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * Not implement yet
 */
class SortByClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = ""
}

object SortByClause extends TreeNodeGenerator[SortByClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): SortByClause = {

    new SortByClause(querySession, parent)
  }
}
