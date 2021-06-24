package org.apache.spark.rqg.ast

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.clauses._

/**
 * This class represents a complete query, each member variable represents a query part. Such as:
 * SelectClause, FromClause, WhereClause, etc.
 */
class Query(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val fromClause: FromClause = generateFromClause

  val selectClause: SelectClause = generateSelectClause

  val whereClauseOption: Option[WhereClause] = generateWhereClauseOption

  val aggregationClauseOption: Option[AggregationClause] = generateAggregationClauseOption
  val havingClauseOption: Option[HavingClause] = generateHavingClauseOption

  val queryOrganization: QueryOrganization = generateQueryOrganization

  private def generateFromClause: FromClause = {
    FromClause(queryContext, Some(this))
  }

  private def generateSelectClause: SelectClause = {
    SelectClause(queryContext, Some(this))
  }

  private def generateWhereClauseOption: Option[WhereClause] = {
    if (RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.WHERE))) {
      Some(WhereClause(queryContext, Some(this)))
    } else {
      None
    }
  }

  private def generateAggregationClauseOption: Option[AggregationClause] = {
    if (selectClause.namedExpressionSeq.exists(_.isAgg) ||
        RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.GROUP_BY))) {
      Some(AggregationClause(queryContext, Some(this)))
    } else {
      None
    }
  }

  private def generateHavingClauseOption: Option[HavingClause] = {
    if (aggregationClauseOption.isDefined &&
      RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.HAVING))) {
      Some(HavingClause(queryContext, Some(this)))
    } else {
      None
    }
  }

  private def generateQueryOrganization: QueryOrganization = {
    QueryOrganization(queryContext, Some(this))
  }

  override def sql: String =
    s"${selectClause.sql} " +
      s"${fromClause.sql} " +
      s"${whereClauseOption.map(_.sql).getOrElse("")} " +
      s" ${aggregationClauseOption.map(_.sql).getOrElse("")} " +
      s" ${havingClauseOption.map(_.sql).getOrElse("")} " +
      s"${queryOrganization.sql} "
}

/**
 * This is the companion object used to generate a Query class.
 * This `generator` object is useful when we want to random choose a generator from an Array.
 */
object Query extends TreeNodeGenerator[Query] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode] = None): Query = {
    new Query(querySession, parent)
  }
}
