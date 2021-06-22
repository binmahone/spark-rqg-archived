package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * relation
 *     : relationPrimary joinRelation*
 *     ;
 */
class Relation(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val relationPrimary: RelationPrimary = generateRelationPrimary
  val joinRelationSeq: Seq[JoinRelation] = generateJoinRelationSeq

  def relations: Seq[RelationPrimary] = Seq(relationPrimary)

  private def generateRelationPrimary: RelationPrimary = {
    val relationPrimary = RelationPrimary(queryContext, Some(this))
    queryContext.availableRelations = queryContext.availableRelations :+ relationPrimary
    relationPrimary
  }

  private def generateJoinRelationSeq: Seq[JoinRelation] = {
    val (min, max) = queryContext.rqgConfig.getBound(RQGConfig.JOIN_COUNT)
    (0 until RandomUtils.choice(min, max)).map { _ =>
      val joinRelation = JoinRelation(queryContext, Some(this))
      queryContext.availableRelations =
        queryContext.availableRelations :+ joinRelation.relationPrimary
      joinRelation
    }
  }

  override def sql: String = s"${relationPrimary.sql} ${joinRelationSeq.map(_.sql).mkString(" ")}"
}

/**
 * Relation Generator
 */
object Relation extends TreeNodeGenerator[Relation] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): Relation = {
    new Relation(querySession, parent)
  }
}
