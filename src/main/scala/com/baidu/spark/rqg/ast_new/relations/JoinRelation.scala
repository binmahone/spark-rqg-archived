package com.baidu.spark.rqg.ast_new.relations

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast_new.{QuerySession, TreeNode, TreeNodeGenerator}

class JoinRelation(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  private val joinTypes = Array("INNER", "CROSS", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER")

  val joinType: String = RandomUtils.choice(joinTypes)
  val relationPrimary: RelationPrimary = RelationPrimary(querySession, Some(this))
  querySession.joiningRelation = Some(relationPrimary)
  val joinCriteria: JoinCriteria = JoinCriteria(querySession, Some(this))
  querySession.joiningRelation = None
  override def sql: String = s"$joinType JOIN ${relationPrimary.sql} ${joinCriteria.sql}"
}

object JoinRelation extends TreeNodeGenerator[JoinRelation] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinRelation = {
    new JoinRelation(querySession, parent)
  }
}
