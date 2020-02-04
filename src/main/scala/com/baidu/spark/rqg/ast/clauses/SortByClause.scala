package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class SortByClause(
    querySession: QuerySession,
    parent: Option[TreeNode]) extends TreeNode {

  override def sql: String = ""
}
