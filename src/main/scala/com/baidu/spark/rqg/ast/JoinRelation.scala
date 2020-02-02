package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RandomUtils

case class JoinRelation(
    querySession: QuerySession,
    parent: Option[TreeNode],
    joinType: String,
    relationPrimary: RelationPrimary,
    joinCriteria: JoinCriteria) extends TreeNode {

  override def sql: String = s"$joinType JOIN ${relationPrimary.sql} ${joinCriteria.sql}"
}

object JoinRelation {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinRelation = {

    val joinRelation = JoinRelation(querySession, parent, null, null, null)

    // LEFT SEMI/ANTI JOIN is invisible for select clause
    // val types = Array("INNER", "CROSS", "LEFT OUTER", "LEFT SEMI", "RIGHT OUTER", "FULL OUTER", "LEFT SEMI")
    val joinTypes = Array("INNER", "CROSS", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER")
    val joinType = RandomUtils.choice(joinTypes)

    val relationPrimary = RelationPrimary(querySession.copy(), Some(joinRelation))

    val joinCriteria = JoinCriteria(
      querySession.copy(joiningRelations = Array(relationPrimary)), Some(joinRelation))

    joinRelation.copy(
      joinType = joinType,
      relationPrimary = relationPrimary,
      joinCriteria = joinCriteria)
  }
}
