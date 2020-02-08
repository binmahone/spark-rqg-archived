package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.RandomUtils
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

  val joinRelationSeq: Seq[JoinRelation] = generateJoinRelationSeq

  def relations: Seq[RelationPrimary] = Seq(relationPrimary)

  private def generateRelationPrimary: RelationPrimary = {
    val relationPrimary = RelationPrimary(querySession, Some(this))
    querySession.availableRelations = querySession.availableRelations :+ relationPrimary
    relationPrimary
  }

  private def generateJoinRelationSeq: Seq[JoinRelation] = {
    (0 until RandomUtils.choice(0, 2)).map { _ =>
      val joinRelation = JoinRelation(querySession, Some(this))
      querySession.availableRelations =
        querySession.availableRelations :+ joinRelation.relationPrimary
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
      querySession: QuerySession,
      parent: Option[TreeNode]): Relation = {
    new Relation(querySession, parent)
  }
}
