package com.baidu.spark.rqg.ast

case class Query(
    querySession: QuerySession,
    parent: Option[TreeNode],
    selectClause: SelectClause,
    fromClause: FromClause) extends TreeNode {

  override def sql: String = s"${selectClause.sql} ${fromClause.sql}"
}

object Query {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): Query = {

    val query = Query(querySession, parent, null, null)

    val fromClause = generateFromClause(querySession, Some(query))

    val selectClause = generateSelectClause(querySession, Some(query), fromClause)

    query.copy(selectClause = selectClause, fromClause = fromClause)
  }

  private def generateFromClause(
      querySession: QuerySession,
      parent: Option[TreeNode]): FromClause = {

    FromClause(querySession.copy(), parent)
  }

  private def generateSelectClause(
      querySession: QuerySession,
      parent: Option[TreeNode],
      fromClause: FromClause): SelectClause = {

    val qs = querySession.copy(availableRelations = fromClause.relations)
    SelectClause(qs, parent)
  }
}
