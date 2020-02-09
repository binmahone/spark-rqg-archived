package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.clauses._

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

  val queryOrganization: QueryOrganization = generateQueryOrganization

  private def generateFromClause: FromClause = {
    FromClause(querySession, Some(this))
  }

  private def generateSelectClause: SelectClause = {
    SelectClause(querySession, Some(this))
  }

  private def generateWhereClauseOption: Option[WhereClause] = {
    if (RandomUtils.nextBoolean()) {
      Some(WhereClause(querySession, Some(this)))
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
