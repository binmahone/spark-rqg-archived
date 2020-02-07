package com.baidu.spark.rqg.ast_new.relations

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast_new.{QuerySession, TreeNode, TreeNodeGenerator}

class Relation(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val relationPrimary: RelationPrimary = RelationPrimary(querySession, Some(this))

  querySession.availableRelations = querySession.availableRelations :+ relationPrimary

  val joinRelationSeq: Seq[JoinRelation] = (0 until RandomUtils.choice(0, 2)).map { _ =>
    val joinRelation = JoinRelation(querySession, Some(this))
    querySession.availableRelations =
      querySession.availableRelations :+ joinRelation.relationPrimary
    joinRelation
  }

  def relations: Seq[RelationPrimary] = joinRelationSeq.map(_.relationPrimary) :+ relationPrimary

  override def sql: String = s"${relationPrimary.sql} ${joinRelationSeq.map(_.sql).mkString(" ")}"
}

object Relation extends TreeNodeGenerator[Relation] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Relation = {
    new Relation(querySession, parent)
  }
}
