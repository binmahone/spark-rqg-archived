package com.baidu.spark.rqg.ast_new.clauses

import com.baidu.spark.rqg.ast_new.{QuerySession, TreeNode, TreeNodeGenerator}

class SelectClause(
  val querySession: QuerySession,
  val parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = "SELECT *"
}

object SelectClause extends TreeNodeGenerator[SelectClause] {
  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode]): SelectClause = {
    new SelectClause(querySession, parent)
  }
}
