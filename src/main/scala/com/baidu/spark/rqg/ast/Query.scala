package com.baidu.spark.rqg.ast

// query
//     : selectClause fromClause whereClause queryOrganization
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
class Query(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val fromClause = new FromClause(querySession)
  val selectClause = new SelectClause(
    querySession.copy(
      primaryRelations = fromClause.relation.joinRelationSeq.map(_.relationPrimary) :+ fromClause.relation.relationPrimary))
  val whereClause: Option[WhereClause] = generateWhereClause
  val queryOrganization = new QueryOrganization(querySession)

  def generateWhereClause: Option[WhereClause] = {
    // if (random.nextBoolean()) Some(new WhereClause(querySession)) else None
    if (false) Some(new WhereClause(querySession)) else None
  }

  def toSql: String = {
    s"${selectClause.toSql} ${fromClause.toSql}" +
      s"${whereClause.map(" " + _.toSql).getOrElse("")}" +
      s" ${queryOrganization.toSql}"
  }
}
