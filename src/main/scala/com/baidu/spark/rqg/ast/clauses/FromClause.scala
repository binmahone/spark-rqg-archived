package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.relations.{Relation, RelationPrimary}

case class FromClause(
    querySession: QuerySession,
    parent: Option[TreeNode],
    relation: Relation) extends TreeNode {
  override def sql: String = s"FROM ${relation.sql}"

  def relations: Array[RelationPrimary] = relation.relations
}

object FromClause {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): FromClause = {

    val fromClause = FromClause(querySession, parent, null)

    val relation = generateRelation(querySession, Some(fromClause))

    fromClause.copy(relation = relation)
  }

  private def generateRelation(
      querySession: QuerySession,
      parent: Option[TreeNode]): Relation = {

    Relation(querySession.copy(), parent)
  }
}
