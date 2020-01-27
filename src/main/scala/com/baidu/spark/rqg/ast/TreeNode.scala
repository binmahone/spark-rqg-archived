package com.baidu.spark.rqg.ast

import scala.util.Random

abstract class TreeNode(querySession: QuerySession) {
  // TODO: visitor pattern is better
  def toSql: String
}

// query
//     : selectClause FROM identifier LIMIT constant
//     ;
class Query(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  // table reference
  val identifier = new Identifier("table", querySession)
  val selectClause = new SelectClause(
    querySession.copy(tables = querySession.tables.filter(_.name == identifier.identifier)))
  // int value
  val constant: Int = random.nextInt(100) + 1

  def toSql: String = {
    s"${selectClause.toSql} FROM ${identifier.toSql} LIMIT $constant"
  }
}

// selectClause
//     : SELECT setQuantifier? identifierSeq
//     ;
class SelectClause(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val setQuantifier: Option[SetQuantifier] = generateSetQuantifier
  val identifierSeq: Seq[Identifier] = generateIdentifierSeq

  def generateSetQuantifier: Option[SetQuantifier] = {
    if (random.nextBoolean()) Some(new SetQuantifier(querySession)) else None
  }

  def generateIdentifierSeq: Seq[Identifier] = {
    val count = random.nextInt(3) + 1
    (0 until count).map(_ => new Identifier("column", querySession))
  }

  def toSql: String = {
    s"SELECT " +
      s"${setQuantifier.map(_.toSql + " ").getOrElse("")}" +
      s"${identifierSeq.map(_.toSql).mkString(",")}"
  }
}

class SetQuantifier(querySession: QuerySession) extends TreeNode(querySession) {
  def toSql: String = "DISTINCT"
}

class Identifier(rule: String, querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val identifier: String = generateIdentifier
  override def toSql: String = identifier

  def generateIdentifier: String = {
    if (rule == "column") {
      val table = querySession.tables(random.nextInt(querySession.tables.length))
      val column = table.columns(random.nextInt(table.columns.length))
      column.name
    } else {
      val table = querySession.tables(random.nextInt(querySession.tables.length))
      table.name
    }
  }
}
