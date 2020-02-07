package com.baidu.spark.rqg.ast_new

import com.baidu.spark.rqg.ast_new.clauses.{FromClause, SelectClause}

class Query(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val fromClause: FromClause = FromClause(querySession, Some(this))
  val selectClause: SelectClause = SelectClause(querySession, Some(this))
  override def sql: String = s"${selectClause.sql} ${fromClause.sql}"
}

object Query extends TreeNodeGenerator [Query] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Query = {
    new Query(querySession, parent)
  }
}
