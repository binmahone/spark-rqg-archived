package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * joinRelation
 *     : (joinType) JOIN right=relationPrimary joinCriteria?
 *     | NATURAL joinType JOIN right=relationPrimary
 *     ;
 *
 * for now we don't support natural join
 */
class JoinRelation(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  private val joinTypes = Array("INNER", "CROSS", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER")

  val joinType: String = RandomUtils.nextChoice(joinTypes)
  val relationPrimary: RelationPrimary = generateRelationPrimary
  val joinCriteria: JoinCriteria = generateJoinCriteria

  private def generateRelationPrimary: RelationPrimary = {
    val relationPrimary = RelationPrimary(querySession, Some(this))
    querySession.joiningRelation = Some(relationPrimary)
    relationPrimary
  }

  private def generateJoinCriteria: JoinCriteria = {
    val joinCriteria = JoinCriteria(querySession, Some(this))
    // Reset querySession's joiningRelation after joinCriteria is generated
    querySession.joiningRelation = None
    joinCriteria
  }
  override def sql: String = s"$joinType JOIN ${relationPrimary.sql} ${joinCriteria.sql}"
}

/**
 * JoinRelation generator
 */
object JoinRelation extends TreeNodeGenerator[JoinRelation] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinRelation = {
    new JoinRelation(querySession, parent)
  }
}
