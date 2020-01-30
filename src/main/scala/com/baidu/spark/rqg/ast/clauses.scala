package com.baidu.spark.rqg.ast

class SelectClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val setQuantifier: Option[SetQuantifier] = generateSetQuantifier
  val identifierSeq: Seq[String] = generateIdentifierSeq

  def generateSetQuantifier: Option[SetQuantifier] = {
    if (random.nextBoolean()) Some(new SetQuantifier(querySession)) else None
  }

  def generateIdentifierSeq: Seq[String] = {
    val count = random.nextInt(3) + 1
    (0 until count).map { _ =>
      val relations = querySession.primaryRelations ++ querySession.joiningRelations
      val relation = relations(random.nextInt(relations.length))
      val columns = querySession.tables.find(_.name == relation.tableIdentifier).get.columns
      val column = columns(random.nextInt(columns.length))
      s"${relation.aliasIdentifier.getOrElse(relation.tableIdentifier)}.${column.name}"
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
  val relation = new Relation(querySession)
  override def toSql: String = s"FROM ${relation.toSql}"
}

class WhereClause(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val booleanExpression = new BooleanExpression(querySession)
  override def toSql: String = s"WHERE ${booleanExpression.toSql}"
}

class QueryOrganization(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val constant: Int = random.nextInt(100) + 1
  override def toSql: String = s"LIMIT $constant"
}
