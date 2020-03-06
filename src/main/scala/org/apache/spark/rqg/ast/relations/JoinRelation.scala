package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}
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
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val joinType: String =
    RandomUtils.choice(joinTypes, queryContext.rqgConfig.getWeight(RQGConfig.JOIN_TYPE)).name
  val relationPrimary: RelationPrimary = generateRelationPrimary
  val joinCriteria: JoinCriteria = generateJoinCriteria

  private def generateRelationPrimary: RelationPrimary = {
    val relationPrimary = RelationPrimary(queryContext, Some(this))
    queryContext.joiningRelation = Some(relationPrimary)
    relationPrimary
  }

  private def generateJoinCriteria: JoinCriteria = {
    val joinCriteria = JoinCriteria(queryContext, Some(this))
    // Reset querySession's joiningRelation after joinCriteria is generated
    queryContext.joiningRelation = None
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
      querySession: QueryContext,
      parent: Option[TreeNode]): JoinRelation = {
    new JoinRelation(querySession, parent)
  }
}
