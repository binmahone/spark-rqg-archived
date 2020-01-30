package com.baidu.spark.rqg.ast

class SelectClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val setQuantifier: Option[SetQuantifier] = generateSetQuantifier
  val identifierSeq: Seq[String] = generateIdentifierSeq

  def generateSetQuantifier: Option[SetQuantifier] = {
    if (random.nextBoolean()) Some(new SetQuantifier(querySession, Some(this))) else None
  }

  def generateIdentifierSeq: Seq[String] = {
    val count = random.nextInt(3) + 1
    val identifiers = (0 until count).map { _ =>
      val relations = querySession.primaryRelations
      val relation = relations(random.nextInt(relations.length))
      val columns = querySession.tables.find(_.name == relation.tableIdentifier).get.columns
      val column = columns(random.nextInt(columns.length))
      s"${relation.aliasIdentifier.getOrElse(relation.tableIdentifier)}.${column.name}"
    }

    parent match {
      case Some(x: Query) if x.aggregationClause.isDefined =>
        val aggregationClause = x.aggregationClause.get
        identifiers.map { i =>
          // TODO: need get column reference from expressions
          if (aggregationClause.groupingExpressions.map(_.toSql).contains(i)) {
            i
          } else {
            s"count($i)"
          }
        }
      case _ => identifiers
    }
  }

  def toSql: String = {
    s"SELECT" +
      s"${setQuantifier.map(" " + _.toSql).getOrElse("")}" +
      s"${identifierSeq.map(" " + _).mkString(",")}"
  }
}

class SetQuantifier(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  def toSql: String = "DISTINCT"
}

class FromClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val relation = new Relation(querySession, Some(this))
  override def toSql: String = s"FROM ${relation.toSql}"
}

class WhereClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val booleanExpression = ValueExpression(querySession, Some(this))
  override def toSql: String = s"WHERE ${booleanExpression.toSql}"
}

class QueryOrganization(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val constant: Int = random.nextInt(100) + 1
  override def toSql: String = s"LIMIT $constant"
}

class AggregationClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val groupingExpressions: Seq[ValueExpression] = generateGroupingExpressions

  def generateGroupingExpressions: Seq[ValueExpression] = {
    val count = random.nextInt(3) + 1
    (0 until count).map { _ =>
      ValueExpression(querySession, Some(this))
    }
  }

  override def toSql: String = s"GROUP BY ${groupingExpressions.map(_.toSql).mkString(",")}"
}
