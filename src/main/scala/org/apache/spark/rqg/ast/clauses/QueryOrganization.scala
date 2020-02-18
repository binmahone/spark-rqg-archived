package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.RandomUtils
import org.apache.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

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
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val limitClauseOption: Option[LimitClause] = generateLimitClause

  val orderByClauseOption: Option[OrderByClause] = generateOrderByClause

  val sortByClauseOption: Option[SortByClause] = generateSortByClause

  private def generateLimitClause: Option[LimitClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(LimitClause(querySession, parent))
    } else {
      None
    }
  }

  private def generateOrderByClause: Option[OrderByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(OrderByClause(querySession, parent))
    } else {
      None
    }
  }

  private def generateSortByClause: Option[SortByClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(SortByClause(querySession, parent))
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
      querySession: QuerySession,
      parent: Option[TreeNode]): QueryOrganization = {

    new QueryOrganization(querySession, parent)
  }
}
