package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.clauses.{AggregationClause, FromClause, SelectClause, WhereClause}

// query
//     : selectClause fromClause whereClause? aggregationClause? queryOrganization
//     ;

// selectClause
//     : SELECT setQuantifier? namedExpressionSeq
//     ;

// setQuantifier
//     : DISTINCT
//     ;

// namedExpressionSeq
//     : namedExpression (',' namedExpression)*
//     ;

// fromClause
//     : FROM relation
//     ;

// relation
//     : relationPrimary joinRelation*
//     ;

// relationPrimary
//     : tableIdentifier (AS tableAlias)?
//     ;

// joinRelation
//     : (joinType) JOIN right=relationPrimary joinCriteria
//     ;

// joinType
//     : INNER? | CROSS | LEFT OUTER? | LEFT? SEMI | RIGHT OUTER? | FULL OUTER? | LEFT? ANTI
//     ;

// joinCriteria
//     : ON booleanExpression
//     ;

// whereClause
//     : WHERE booleanExpression
//     ;

// aggregationClause
//     : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)*
//     ;

// queryOrganization
//     : LIMIT constant
//     ;

// namedExpression
//     : expression (AS alias)?

// expression
//     : booleanExpression
//     ;

// booleanExpression
//     : NOT booleanExpression                                            #logicalNot
//     : valueExpression predicate?                                       #predicated
//     ;

// predicate
//     : IS NOT? kind=NULL
//     | NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
//     ;

// valueExpression
//     : primaryExpression
//     | left=valueExpression operator=(PLUS | MINUS) right=valueExpression       #arithmeticBinary
//     | left=valueExpression comparisonOperator right=valueExpression            #comparison
//     ;

// primaryExpression
//     : constant
//     | identifier                                                       #columnReference

// comparisonOperator
//     : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
//     ;

case class Query(
    querySession: QuerySession,
    parent: Option[TreeNode],
    selectClause: SelectClause,
    whereClauseOption: Option[WhereClause],
    aggregationClauseOption: Option[AggregationClause],
    fromClause: FromClause) extends TreeNode {

  override def sql: String =
    s"${selectClause.sql}" +
    s" ${fromClause.sql}" +
    s" ${whereClauseOption.map(_.sql).getOrElse("")}" +
    s" ${aggregationClauseOption.map(_.sql).getOrElse("")}"
}

object Query {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): Query = {

    val query = Query(querySession, parent, null, null, null, null)

    val fromClause = generateFromClause(querySession, Some(query))

    val selectClause = generateSelectClause(querySession, Some(query), fromClause)

    val whereClauseOption = generateWhereClause(querySession, Some(query), fromClause)

    val aggregationClauseOption = generateAggregationClause(querySession, Some(query), fromClause)

    query.copy(
      selectClause = selectClause,
      fromClause = fromClause,
      whereClauseOption = whereClauseOption,
      aggregationClauseOption = aggregationClauseOption)
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

  private def generateWhereClause(
      querySession: QuerySession,
      parent: Option[TreeNode],
      fromClause: FromClause): Option[WhereClause] = {

    if (RandomUtils.nextBoolean()) {
      val qs = querySession.copy(availableRelations = fromClause.relations)
      Some(WhereClause(qs, parent))
    } else {
      None
    }
  }

  private def generateAggregationClause(
      querySession: QuerySession,
      parent: Option[TreeNode],
      fromClause: FromClause): Option[AggregationClause] = {

    if (RandomUtils.nextBoolean()) {
      val qs = querySession.copy(availableRelations = fromClause.relations)
      Some(AggregationClause(qs, parent))
    } else {
      None
    }
  }
}
