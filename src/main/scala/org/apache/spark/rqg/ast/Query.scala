package org.apache.spark.rqg.ast

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.clauses._

/**
 * This class represents a complete query, each member variable represents a query part. Such as:
 * SelectClause, FromClause, WhereClause, etc.
 */
class Query(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val fromClause: FromClause = generateFromClause

  val selectClause: SelectClause = generateSelectClause

  val whereClauseOption: Option[WhereClause] = generateWhereClauseOption

  val aggregationClauseOption: Option[AggregationClause] = generateAggregationClauseOption

  val queryOrganization: QueryOrganization = generateQueryOrganization

  private def generateFromClause: FromClause = {
    FromClause(querySession, Some(this))
  }

  private def generateSelectClause: SelectClause = {
    SelectClause(querySession, Some(this))
  }

  private def generateWhereClauseOption: Option[WhereClause] = {
    if (RandomUtils.nextBoolean(querySession.rqgConfig.getProbability(RQGConfig.WHERE))) {
      Some(WhereClause(querySession, Some(this)))
    } else {
      None
    }
  }

  private def generateAggregationClauseOption: Option[AggregationClause] = {
    if (selectClause.namedExpressionSeq.exists(_.isAgg) ||
        RandomUtils.nextBoolean(querySession.rqgConfig.getProbability(RQGConfig.GROUP_BY))) {
      Some(AggregationClause(querySession, Some(this)))
    } else {
      None
    }
  }

  private def generateQueryOrganization: QueryOrganization = {
    QueryOrganization(querySession, Some(this))
  }

  override def sql: String =
    s"${selectClause.sql} " +
      s"${fromClause.sql} " +
      s"${whereClauseOption.map(_.sql).getOrElse("")} " +
      s" ${aggregationClauseOption.map(_.sql).getOrElse("")} " +
      s"${queryOrganization.sql} "
}

/**
 * This is the companion object used to generate a Query class.
 * This `generator` object is useful when we want to random choose a generator from an Array.
 */
object Query extends TreeNodeGenerator[Query] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): Query = {
    new Query(querySession, parent)
  }
}
