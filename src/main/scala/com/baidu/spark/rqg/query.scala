package com.baidu.spark.rqg

case class Query(
    selectClause: SelectClause,
    fromClause: FromClause,
    withClause: Option[WithClause] = None,
    whereClause: Option[WhereClause] = None,
    groupByClause: Option[GroupByClause] = None,
    havingClause: Option[HavingClause] = None,
    unionClause: Option[UnionClause] = None,
    orderByClause: Option[OrderByClause] = None,
    limitClause: Option[LimitClause] = None,
    parent: Option[Query] = None) {
  def isNestedQuery: Boolean = parent.isDefined
}

object Query {
  class Builder {

    var selectClause: SelectClause = _
    var fromClause: FromClause = _

    def build(): Query = {

      if (selectClause == null || fromClause == null) {
        throw new IllegalArgumentException("selectClause or fromClause can't be empty")
      }
      new Query(selectClause, fromClause)
    }

  }
  def builder(): Builder = new Builder
}

case class SelectClause(items: Array[SelectItem], distinct: Boolean = false)

case class FromClause(tableExpr: TableExpr, joinClauses: Array[JoinClause] = Array.empty)

class InlineView(query: String, alias: String, isVisible: Boolean)

case class WithClause(withClauseInlineView: WithClauseInlineView)

class WithClauseInlineView(
    query: String,
    alias: String,
    isVisible: Boolean)
  extends InlineView(query, alias, isVisible)

case class JoinClause(joinType: String, tableExpr: String, booleanExpr: String)

case class WhereClause(booleanExpr: String)

case class GroupByClause(groupByItems: Array[String])

case class HavingClause(booleanExpr: String)

case class UnionClause(query: String, all: Boolean)

case class OrderByClause(valExpr: String)

case class LimitClause(limit: Int)

case class SelectItem(valExpr: ValExpr)

