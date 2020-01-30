package com.baidu.spark.rqg.ast

import scala.collection.mutable.ArrayBuffer

class Relation(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {

  val relationPrimary = new RelationPrimary(querySession)
  val joinRelationSeq: Array[JoinRelation] = generateJoinRelationSeq

  def generateJoinRelationSeq: Array[JoinRelation] = {
    val joinCount = random.nextInt(querySession.tables.length)
    val selectedTables = new ArrayBuffer[RelationPrimary]()
    var qs = querySession
    selectedTables.append(relationPrimary)
    (0 until joinCount).map { _ =>
      qs = qs.copy(primaryRelations = selectedTables.toArray)
      val joinRelation = new JoinRelation(qs)
      selectedTables.append(joinRelation.relationPrimary)
      joinRelation
    }.toArray
  }

  override def toSql: String = s"${relationPrimary.toSql}" +
    s"${joinRelationSeq.map(" " + _.toSql).mkString("")}"
}

class RelationPrimary(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val tableIdentifier: String = generateTableIdentifier
  val aliasIdentifier: Option[String] = generateAliasIdentifier

  def generateTableIdentifier: String = {
    querySession.tables(random.nextInt(querySession.tables.length)).name
  }
  def generateAliasIdentifier: Option[String] = {
    // For now, it's always be true to avoid name conflict with joined tables
    if (true) Some(s"alias${querySession.nextAliasId.toString}") else None
  }

  override def toSql: String = s"$tableIdentifier" +
    s"${aliasIdentifier.map(" AS " + _).getOrElse("")}"
}

class JoinRelation(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val joinType = new JoinType(querySession)
  val relationPrimary = new RelationPrimary(querySession)
  val joinCriteria: Option[JoinCriteria] = generateJoinCriteria

  def generateJoinCriteria: Option[JoinCriteria] = {
    val qs = querySession.copy(joiningRelations = Array(relationPrimary))
    // TODO: always true for debug purpose
    // if (random.nextBoolean()) Some(new JoinCriteria(qs)) else None
    if (true) Some(new JoinCriteria(qs)) else None
  }

  override def toSql: String = s"${joinType.toSql} JOIN ${relationPrimary.toSql}" +
    s"${joinCriteria.map(" " + _.toSql).getOrElse("")}"
}

class JoinCriteria(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  val booleanExpression = new BooleanExpression(querySession)
  override def toSql: String = s"ON ${booleanExpression.toSql}"
}

class JoinType(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  // LEFT SEMI/ANTI JOIN is invisible for select clause
  // val types = Array("INNER", "CROSS", "LEFT OUTER", "LEFT SEMI", "RIGHT OUTER", "FULL OUTER", "LEFT SEMI")
  val types = Array("INNER", "CROSS", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER")
  val joinType = types(random.nextInt(types.length))

  override def toSql: String = joinType
}
