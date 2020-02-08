package com.baidu.spark.rqg.ast

/**
 * This class represents a complete query, each member variable represents a query part. Such as:
 * SelectClause, FromClause, WhereClause, etc.
 */
class Query(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {
  override def sql: String = "SELECT * FROM table_1"
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
