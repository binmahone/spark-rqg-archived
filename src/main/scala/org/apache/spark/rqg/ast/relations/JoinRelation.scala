package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}
import org.apache.spark.rqg.ast.joins._

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

  val joinType: String =
    RandomUtils.choice(joinTypes, querySession.rqgConfig.getWeight(RQGConfig.JOIN_TYPE)).name
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

  private def joinTypes = Array(INNER, CROSS, LEFT, RIGHT, FULL)
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
