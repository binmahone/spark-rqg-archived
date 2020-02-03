package com.baidu.spark.rqg.ast.relations

import scala.collection.mutable.ArrayBuffer

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast._

case class Relation(
    querySession: QuerySession,
    parent: Option[TreeNode],
    relationPrimary: RelationPrimary,
    joinRelationSeq: Array[JoinRelation]) extends TreeNode {

  def relations: Array[RelationPrimary] = joinRelationSeq.map(_.relationPrimary) :+ relationPrimary

  override def sql: String = s"${relationPrimary.sql} ${joinRelationSeq.map(_.sql).mkString(" ")}"
}

object Relation {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Relation = {

    val relation = Relation(querySession, parent, null, null)

    val relationPrimary = generateRelationPrimary(querySession, parent)

    val joinRelationSeq = generateJoinRelationSeq(
      relationPrimary.querySession.copy(), parent, relationPrimary)

    relation.copy(relationPrimary = relationPrimary, joinRelationSeq = joinRelationSeq)
  }

  private def generateRelationPrimary(querySession: QuerySession, parent: Option[TreeNode]) = {
    RelationPrimary(querySession.copy(), parent)
  }

  private def generateJoinRelationSeq(
      querySession: QuerySession, parent: Option[TreeNode], relationPrimary: RelationPrimary) = {

    val availableRelations = new ArrayBuffer[RelationPrimary]()
    availableRelations.append(relationPrimary)

    (0 until RandomUtils.choice(0, 10)).map { _ =>
      val joinRelation = JoinRelation(
        querySession.copy(availableRelations = availableRelations.toArray), parent)

      availableRelations.append(joinRelation.relationPrimary)

      joinRelation
    }.toArray
  }
}
