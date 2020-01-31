package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{DataType, ValueGenerator}

// TODO: clarify expression type
// BooleanExpression should be bool operation, such as AND, OR, NOT, EXISTS
// ValueExpression should be value operation, such as PLUS, MINUS, EQ, GT
// primaryExpression should be the real value, such as column, constant, function

class ColumnComparison(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends ValueExpression(querySession, parent) {
  // Step 1: random pick a common data type
  // Think: what if there is no common data type?
  private val dataType: DataType[_] = {
    val leftDataTypes = querySession.primaryRelations.flatMap(relation =>
      querySession.tables.find(_.name == relation.tableIdentifier)
    ).flatMap(_.columns).map(_.dataType)

    val rightDataTypes = querySession.joiningRelations.flatMap(relation =>
      querySession.tables.find(_.name == relation.tableIdentifier)
    ).flatMap(_.columns).map(_.dataType)

    val dataTypes = leftDataTypes.intersect(rightDataTypes).distinct
    assert(dataTypes.nonEmpty,
      "left and right relations has no common data type to join")
    dataTypes(random.nextInt(dataTypes.length))
  }

  // Step 2: random pick a table has one column with target data type
  private val leftRelations = querySession.primaryRelations.filter { relation =>
    querySession.tables
      .find(_.name == relation.tableIdentifier)
      .get.columns
      .exists(_.dataType == dataType)
  }
  private val leftRelation = leftRelations(random.nextInt(leftRelations.length))

  private val rightRelations = querySession.joiningRelations.filter { relation =>
    querySession.tables
      .find(_.name == relation.tableIdentifier)
      .get.columns
      .exists(_.dataType == dataType)
  }
  private val rightRelation = rightRelations(random.nextInt(rightRelations.length))

  // Step 3: random pick a column with target data type
  private val leftColumns = querySession.tables.find(_.name == leftRelation.tableIdentifier)
    .get.columns.toArray
  private val leftColumn = leftColumns(random.nextInt(leftColumns.length))

  private val rightColumns = querySession.tables.find(_.name == rightRelation.tableIdentifier)
    .get.columns.toArray
  private val rightColumn = rightColumns(random.nextInt(rightColumns.length))

  // Step 4: set column identifier
  val left = s"${leftRelation.aliasIdentifier.getOrElse(leftRelation.tableIdentifier)}.${leftColumn.name}"
  val right = s"${rightRelation.aliasIdentifier.getOrElse(rightRelation.tableIdentifier)}.${rightColumn.name}"

  val comparator: String = "=="

  override def toSql: String = s"$left $comparator $right"
}

class ConstantComparison(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends ValueExpression(querySession, parent) {

  val valueGenerator = new ValueGenerator(random)
  // Step 1: pick a column randomly
  private val relations = querySession.primaryRelations
  private val relation = relations(random.nextInt(relations.length))
  private val columns = querySession.tables.find(_.name == relation.tableIdentifier).get.columns
  private val column = columns(random.nextInt(columns.length))
  private val dataType = column.dataType

  val left: String = s"${relation.aliasIdentifier.getOrElse(relation.tableIdentifier)}.${column.name}"

  val right: String = valueGenerator.generateValue(dataType).toString

  val comparator: String = "=="

  override def toSql: String = s"$left $comparator $right"
}

class PrimaryExpression(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends ValueExpression(querySession, parent) {

  private val relations = querySession.primaryRelations
  private val relation = relations(random.nextInt(relations.length))
  private val columns = querySession.tables.find(_.name == relation.tableIdentifier).get.columns
  private val column = columns(random.nextInt(columns.length))
  val expression = s"${relation.aliasIdentifier.getOrElse(relation.tableIdentifier)}.${column.name}"
  override def toSql: String = expression
}

abstract class ValueExpression(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends TreeNode(querySession, parent)

object ValueExpression {
  def apply(querySession: QuerySession, parent: Option[TreeNode] = None): ValueExpression = {
    parent match {
      case Some(_: JoinCriteria) =>
        // For JoinCriteria, left and right should come from left and right relation.
        new ColumnComparison(querySession, parent)
      case Some(_ : AggregationClause) =>
        new PrimaryExpression(querySession, parent)
      case _ =>
        new ConstantComparison(querySession, parent)
    }
  }
}