package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * Not implement yet
 */
class SortByClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = ""
}

object SortByClause extends TreeNodeGenerator[SortByClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): SortByClause = {

    new SortByClause(querySession, parent)
  }
}