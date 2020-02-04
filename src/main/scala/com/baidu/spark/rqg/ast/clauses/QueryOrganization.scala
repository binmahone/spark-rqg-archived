package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class QueryOrganization(
    querySession: QuerySession,
    parent: Option[TreeNode],
    limitClauseOption: Option[LimitClause],
    orderByClauseOption: Option[OrderByClause],
    sortByClauseOption: Option[SortByClause]) extends TreeNode {

  override def sql: String = s"${orderByClauseOption.map(_.sql).getOrElse("")}" +
    s" ${sortByClauseOption.map(_.sql).getOrElse("")}" +
    s" ${limitClauseOption.map(_.sql).getOrElse("")}"
}

object QueryOrganization {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): QueryOrganization = {

    val queryOrganization = QueryOrganization(querySession, parent, null, null, null)

    val limitClauseOption = generateLimitClause(querySession, Some(queryOrganization))
    val orderByClauseOption = generateOrderByClause(querySession, Some(queryOrganization))
    val sortByClauseOption = generateSortByClause(querySession, Some(queryOrganization))

    queryOrganization.copy(
      limitClauseOption = limitClauseOption,
      orderByClauseOption = orderByClauseOption,
      sortByClauseOption = sortByClauseOption
    )
  }

  private def generateLimitClause(
      querySession: QuerySession, parent: Option[TreeNode]): Option[LimitClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(LimitClause(querySession, parent))
    } else {
      None
    }
  }

  private def generateOrderByClause(
      querySession: QuerySession, parent: Option[TreeNode]): Option[OrderByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(OrderByClause(querySession, parent))
    } else {
      None
    }
  }

  private def generateSortByClause(
      querySession: QuerySession, parent: Option[TreeNode]): Option[SortByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(SortByClause(querySession, parent))
    } else {
      None
    }
  }
}
