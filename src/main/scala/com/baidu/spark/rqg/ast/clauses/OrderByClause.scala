package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * Not implement yet
 */
class OrderByClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = ""
}

object OrderByClause extends TreeNodeGenerator[OrderByClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): OrderByClause = {

    new OrderByClause(querySession, parent)
  }
}