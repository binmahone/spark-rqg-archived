package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * relation
 *     : relationPrimary joinRelation*
 *     ;
 */
class Relation(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val relationPrimary: RelationPrimary = generateRelationPrimary

  def relations: Seq[RelationPrimary] = Seq(relationPrimary)

  private def generateRelationPrimary: RelationPrimary = {
    RelationPrimary(querySession, Some(this))
  }

  override def sql: String = s"${relationPrimary.sql}"
}

/**
 * Relation Generator
 */
object Relation extends TreeNodeGenerator[Relation] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Relation = {
    new Relation(querySession, parent)
  }
}
