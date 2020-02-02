package com.baidu.spark.rqg.ast

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
//     : valueExpression
//     ;

// valueExpression
//     : primaryExpression
//     | left=valueExpression comparisonOperator right=valueExpression  #comparison
//     ;

// comparisonOperator
//     : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
//     ;

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
