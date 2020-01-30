package com.baidu.spark.rqg.ast

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.baidu.spark.rqg.{DataType, RQGColumn, RQGTable}

abstract class TreeNode(querySession: QuerySession) {
  // TODO: visitor pattern is better
  def toSql: String
}

// query
//     : selectClause fromClause whereClause queryOrganization
//     ;

// fromClause
//     : FROM relation
//     ;

// whereClause
//     : WHERE booleanExpression

// selectClause
//     : SELECT setQuantifier? identifierSeq
//     ;

// queryOrganization
//     : LIMIT constant
//     ;

// relation
//     : identifier joinRelation*
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
//     : left=identifier '==' right=identifier
//     | left=identifier '==' constant
//     ;

// TODO #1: column reference needs table prefix - done
// TODO #2: joined columns should have same type - done
// TODO #3: selected columns should belong to tables in from clause - done
// TODO #4: joined columns should belong to left and right tables - done
// TODO #5: join condition data type should be common data type - done
// TODO #6: table should have alias - done
class Query(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val fromClause = new FromClause(querySession)
  val selectClause = new SelectClause(
    querySession.copy(
      selectedTables = fromClause.relation.joinRelationSeq.map(_.relationPrimary) :+ fromClause.relation.relationPrimary))
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

class FromClause(querySession: QuerySession) extends TreeNode(querySession) {
  val relation = new Relation(querySession)
  override def toSql: String = s"FROM ${relation.toSql}"
}

class WhereClause(querySession: QuerySession) extends TreeNode(querySession) {
  val booleanExpression = new BooleanExpression(querySession)
  override def toSql: String = s"WHERE ${booleanExpression.toSql}"
}

class QueryOrganization(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val constant: Int = random.nextInt(100) + 1
  override def toSql: String = s"LIMIT $constant"
}

class Relation(querySession: QuerySession) extends TreeNode(querySession) {

  val random = new Random()
  val relationPrimary = new RelationPrimary(querySession)
  val joinRelationSeq: Array[JoinRelation] = generateJoinRelationSeq

  def generateJoinRelationSeq: Array[JoinRelation] = {
    val joinCount = random.nextInt(querySession.tables.length)
    val selectedTables = new ArrayBuffer[RelationPrimary]()
    var qs = querySession
    selectedTables.append(relationPrimary)
    (0 until joinCount).map { _ =>
      qs = qs.copy(selectedTables = selectedTables.toArray)
      val joinRelation = new JoinRelation(qs)
      selectedTables.append(joinRelation.relationPrimary)
      joinRelation
    }.toArray
  }

  override def toSql: String = s"${relationPrimary.toSql}" +
    s"${joinRelationSeq.map(" " + _.toSql).mkString("")}"
}

class RelationPrimary(querySession: QuerySession) extends TreeNode(querySession) {
  val tableIdentifier = new Identifier(querySession, "table")
  val alias: Option[Identifier] = generateAlias

  def generateAlias: Option[Identifier] = {
    // For now, it's always be true to avoid name conflict with joined tables
    if (true) Some(new Identifier(querySession, "alias")) else None
  }

  override def toSql: String = s"${tableIdentifier.toSql}" +
    s"${alias.map(" AS " + _.toSql).getOrElse("")}"

}

class JoinRelation(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val joinType = new JoinType(querySession)
  val relationPrimary = new RelationPrimary(querySession)
  val joinCriteria: Option[JoinCriteria] = generateJoinCriteria

  def generateJoinCriteria: Option[JoinCriteria] = {
    val qs = querySession.copy(joiningTables = Array(relationPrimary))
    // TODO: always true for debug purpose
    // if (random.nextBoolean()) Some(new JoinCriteria(qs)) else None
    if (true) Some(new JoinCriteria(qs)) else None
  }

  override def toSql: String = s"${joinType.toSql} JOIN ${relationPrimary.toSql}" +
    s"${joinCriteria.map(" " + _.toSql).getOrElse("")}"
}

class JoinCriteria(querySession: QuerySession) extends TreeNode(querySession) {
  val booleanExpression = new BooleanExpression(querySession)
  override def toSql: String = s"ON ${booleanExpression.toSql}"
}

class BooleanExpression(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val dataType: DataType[_] = {
    val leftDataTypes = querySession.selectedTables.flatMap { relaion =>
      querySession.tables.find(_.name == relaion.tableIdentifier.identifier)
    }.flatMap(_.columns).map(_.dataType).toSet

    val rightDataTypes = querySession.joiningTables.flatMap { relaion =>
      querySession.tables.find(_.name == relaion.tableIdentifier.identifier)
    }.flatMap(_.columns).map(_.dataType).toSet

    val dataTypes = leftDataTypes.intersect(rightDataTypes).toArray
    dataTypes(random.nextInt(dataTypes.length))
  }
  val left = new Identifier(
    querySession,
    "column",
    relationPredicate = r => querySession.selectedTables.contains(r),
    columnPredicate = c => c.dataType == dataType)

  val right = new Identifier(
    querySession,
    "column",
    relationPredicate = r => querySession.joiningTables.contains(r),
    columnPredicate = c => c.dataType == dataType)

  override def toSql: String = s"${left.toSql} == ${right.toSql}"
}

class JoinType(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  // LEFT SEMI/ANTI JOIN is invisible for select clause
  // val types = Array("INNER", "CROSS", "LEFT OUTER", "LEFT SEMI", "RIGHT OUTER", "FULL OUTER", "LEFT SEMI")
  val types = Array("INNER", "CROSS", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER")
  val joinType = types(random.nextInt(types.length))

  override def toSql: String = joinType
}

class SelectClause(querySession: QuerySession) extends TreeNode(querySession) {
  val random = new Random()
  val setQuantifier: Option[SetQuantifier] = generateSetQuantifier
  val identifierSeq: Seq[Identifier] = generateIdentifierSeq

  def generateSetQuantifier: Option[SetQuantifier] = {
    if (random.nextBoolean()) Some(new SetQuantifier(querySession)) else None
  }

  def generateIdentifierSeq: Seq[Identifier] = {
    val count = random.nextInt(3) + 1
    (0 until count).map(_ => new Identifier(querySession, "column"))
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

class Identifier(
  querySession: QuerySession,
  identifierType: String,
  tablePredicate: RQGTable => Boolean = _ => true,
  relationPredicate: RelationPrimary => Boolean = _ => true,
  columnPredicate: RQGColumn => Boolean = _ => true)
  extends TreeNode(querySession) {
  val random = new Random()
  val identifier: String = generateIdentifier
  override def toSql: String = identifier

  def generateIdentifier: String = {
    if (identifierType == "column") {
      val relations = (querySession.selectedTables ++ querySession.joiningTables).filter(relationPredicate)
      val relation = relations(random.nextInt(relations.length))
      val columns = querySession.tables.find(_.name == relation.tableIdentifier.identifier).get.columns.filter(columnPredicate)
      val column = columns(random.nextInt(columns.length))
      s"${relation.alias.getOrElse(relation.tableIdentifier).identifier}.${column.name}"
    } else if (identifierType == "table") {
      val tables = querySession.tables.filter(tablePredicate)
      val table = tables(random.nextInt(tables.length))
      table.name
    } else {
      // alias
      s"alias${querySession.nextAliasId.toString}"
    }
  }
}

// Use 2 conditions: join condition and where condition ???
// how to deal with nested booleanExpression
// booleanExpression type: column cmp column, column cmp constant ...
