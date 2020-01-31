package com.baidu.spark.rqg.ast

// query
//     : selectClause fromClause whereClause? aggregationClause? queryOrganization
//     ;

// fromClause
//     : FROM relation
//     ;

// whereClause
//     : WHERE booleanExpression

// selectClause
//     : SELECT setQuantifier? columnIdentifierSeq
//     ;

// queryOrganization
//     : LIMIT constant
//     ;

// relation
//     : tableIdentifier (AS alias)? joinRelation*
//     ;

// joinRelation
//     : joinType JOIN right=relationPrimary joinCriteria?

// joinType
//     : INNER?
//     | CROSS
//     | LEFT OUTER?
//     | LEFT? SEMI
//     | RIGHT OUTER?
//     | FULL OUTER?
//     | LEFT? ANTI
//     ;

// joinCriteria
//     : ON booleanExpression

// booleanExpression
//     : left=columnIdentifier '==' right=columnIdentifier
//     | left=columnIdentifier '==' right=constant
//     ;
class Query(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends TreeNode(querySession, parent) {

  val fromClause: FromClause = generateFromClause

  private val querySessionWithRelations =
    querySession.copy(primaryRelations = fromClause.relation.primaryRelations)

  val whereClause: Option[WhereClause] = generateWhereClauseOption

  val aggregationClause: Option[AggregationClause] = generateAggregationClauseOption

  val selectClause: SelectClause = generateSelectClause

  val queryOrganization: QueryOrganization = generateQueryOrganization

  def generateFromClause: FromClause = {
    new FromClause(querySession, Some(this))
  }

  def generateWhereClauseOption: Option[WhereClause] = {
    // if (random.nextBoolean()) {
    if (true) { // always be true for debugging
      Some(new WhereClause(querySessionWithRelations, Some(this)))
    } else {
      None
    }
  }

  def generateAggregationClauseOption: Option[AggregationClause] = {
    if (random.nextBoolean()) {
      Some(new AggregationClause(querySessionWithRelations, Some(this)))
    } else {
      None
    }
  }

  def generateSelectClause: SelectClause = {
    new SelectClause(querySessionWithRelations, Some(this))
  }

  def generateQueryOrganization: QueryOrganization = {
    new QueryOrganization(querySessionWithRelations, Some(this))
  }

  def toSql: String = {
    s"${selectClause.toSql} ${fromClause.toSql}" +
      s"${whereClause.map(" " + _.toSql).getOrElse("")}" +
      s"${aggregationClause.map(" " + _.toSql).getOrElse("")}" +
      s" ${queryOrganization.toSql}"
  }
}
