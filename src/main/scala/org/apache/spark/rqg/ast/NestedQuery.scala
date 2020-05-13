package org.apache.spark.rqg.ast

import org.apache.spark.rqg.{DataType, RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.clauses._
import org.apache.spark.rqg.ast.expressions.PrimaryExpression

/**
 * This class represents a complete SubQuery, it's just like Query but have some added constraint.
 * This class was created to separate responsibility from Query because SubQuery is somewhat needs
 * and independent/separate context.
 */
class NestedQuery(
     val queryContext: QueryContext,
     val parent: Option[TreeNode],
     val requiredDataType: Option[DataType[_]]) extends TreeNode {

  // Decrease this so we don't generate subquery indefinitely
  queryContext.allowedNestedSubQueryCount -= 1

  val fromClause: FromClause = generateFromClause

  val prevAggPref = queryContext.aggPreference
  val prevAllowNestedExpressionCount = queryContext.allowedNestedExpressionCount

  // When in primary expression, the subQuery should not return more than
  // two rows so we have to aggregate it
  if (parent.get.isInstanceOf[PrimaryExpression]) {
    queryContext.aggPreference = AggPreference.PREFER
    queryContext.allowedNestedExpressionCount = 1
  }

  val selectClause: SelectClause = generateSelectClause

  if (parent.get.isInstanceOf[PrimaryExpression]) {
    queryContext.aggPreference = prevAggPref
    queryContext.allowedNestedExpressionCount = prevAllowNestedExpressionCount
  }

  val whereClauseOption: Option[WhereClause] = generateWhereClauseOption

  val aggregationClauseOption: Option[AggregationClause] = generateAggregationClauseOption

  val queryOrganization: QueryOrganization = generateQueryOrganization

  private def generateFromClause: FromClause = {
    FromClause(queryContext, Some(this))
  }

  private def generateSelectClause: SelectClause = {
    SelectClause(queryContext, Some(this), requiredDataType)
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

  private def generateQueryOrganization: QueryOrganization = {
    QueryOrganization(queryContext, Some(this))
  }

  override def sql: String =
    s"${selectClause.sql} " +
      s"${fromClause.sql} " +
      s"${whereClauseOption.map(_.sql).getOrElse("")} " +
      s" ${aggregationClauseOption.map(_.sql).getOrElse("")} " +
      s"${queryOrganization.sql} "
}

/**
 * This is the companion object used to generate a NestedQuery class.
 * This `generator` object is useful when we want to random choose a generator from an Array.
 */
object NestedQuery extends TreeNodeWithParent[NestedQuery] {
  def apply(querySession: QueryContext,
            parent: Option[TreeNode] = None,
            requiredDataType: Option[DataType[_]] = None): NestedQuery = {
    new NestedQuery(querySession, parent, requiredDataType)
  }
}

