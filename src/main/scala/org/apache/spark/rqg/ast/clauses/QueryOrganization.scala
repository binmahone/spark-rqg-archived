package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.RandomUtils
import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * queryOrganization
 *     : (ORDER BY order+=sortItem (',' order+=sortItem)*)?
 *       (CLUSTER BY clusterBy+=expression (',' clusterBy+=expression)*)?
 *       (DISTRIBUTE BY distributeBy+=expression (',' distributeBy+=expression)*)?
 *       (SORT BY sort+=sortItem (',' sort+=sortItem)*)?
 *       windowClause?
 *       (LIMIT (ALL | limit=expression))?
 *     ;
 *
 * For now, only support limit clause
 */
class QueryOrganization(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val limitClauseOption: Option[LimitClause] = generateLimitClause

  val orderByClauseOption: Option[OrderByClause] = generateOrderByClause

  val sortByClauseOption: Option[SortByClause] = generateSortByClause

  private def generateLimitClause: Option[LimitClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(LimitClause(queryContext, parent))
    } else {
      None
    }
  }

  private def generateOrderByClause: Option[OrderByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(OrderByClause(queryContext, parent))
    } else {
      None
    }
  }

  private def generateSortByClause: Option[SortByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(SortByClause(queryContext, parent))
    } else {
      None
    }
  }

  override def sql: String =
    s"${orderByClauseOption.map(_.sql).getOrElse("")} " +
    s"${sortByClauseOption.map(_.sql).getOrElse("")} " +
    s"${limitClauseOption.map(_.sql).getOrElse("")}"
}

/**
 * QueryOrganization generator
 */
object QueryOrganization extends TreeNodeGenerator[QueryOrganization] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): QueryOrganization = {

    new QueryOrganization(querySession, parent)
  }
}
