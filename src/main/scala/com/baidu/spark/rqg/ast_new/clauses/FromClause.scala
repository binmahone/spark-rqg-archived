package com.baidu.spark.rqg.ast_new.clauses

import com.baidu.spark.rqg.ast_new.relations.Relation
import com.baidu.spark.rqg.ast_new.{QuerySession, TreeNode, TreeNodeGenerator}

class FromClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val relation: Relation = Relation(querySession, Some(this))
  override def sql: String = s"FROM ${relation.sql}"
}

object FromClause extends TreeNodeGenerator[FromClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): FromClause = {
    new FromClause(querySession, parent)
  }
}
