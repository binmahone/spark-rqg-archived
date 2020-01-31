package com.baidu.spark.rqg.ast

import scala.util.Random

import com.baidu.spark.rqg.new_ast.PrimaryExpression
import com.baidu.spark.rqg.{DataType, ValueGenerator}

// expression
//     : booleanExpression
//     ;
//
// booleanExpression
//     : NOT booleanExpression                                        #logicalNot
//     | valueExpression predicate?                                   #predicated
//     | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
//     | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
//     ;
//
// predicate
//     : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
//     | NOT? kind=IN '(' expression (',' expression)* ')'
//     | NOT? kind=(RLIKE | LIKE) pattern=valueExpression
//     | IS NOT? kind=NULL
//     ;
//
// valueExpression
//     : primaryExpression                                                                      #valueExpressionDefault
//     | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
//     | left=valueExpression comparisonOperator right=valueExpression                          #comparison
//     ;
//
// primaryExpression
//     | constant                                                                                 #constantDefault
//     | ASTERISK                                                                                 #star
//     | identifier                                                                               #columnReference
//     ;

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

abstract class BooleanExpression(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends TreeNode(querySession, parent) {

  querySession.nestedExpressionCount += 1
}

object BooleanExpression {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): BooleanExpression = {

    val maxNestedCount = 1
    val random = new Random()
    random.nextInt(3) match {
      case 0 if querySession.nestedExpressionCount <= maxNestedCount =>
        new LogicalNot(querySession, parent)
      case 1 if querySession.nestedExpressionCount <= maxNestedCount =>
        new LogicalBinary(querySession, parent)
      case _ =>
        new Predicated(querySession, parent)
    }
  }
}

class LogicalNot(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends BooleanExpression(querySession, parent) {

  val booleanExpression = BooleanExpression(querySession, parent)
  override def toSql: String = s"NOT ${booleanExpression.toSql}"
}

class Predicated(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends BooleanExpression(querySession, parent) {

  val valueExpression = ValueExpression2(querySession, parent)
  val predicateOption: Option[Predicate] = generatePredicateOption

  def generatePredicateOption: Option[Predicate] = {
    val maxNestedCount = 1
    if (random.nextBoolean() && querySession.nestedExpressionCount <= maxNestedCount) {
      Some(Predicate(querySession, parent))
    } else {
      None
    }
  }

  override def toSql: String = s"${valueExpression.toSql}" +
    s"${predicateOption.map(" " + _.toSql).getOrElse("")}"
}

class LogicalBinary(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends BooleanExpression(querySession, parent) {

  val left = BooleanExpression(querySession, parent)
  val right = BooleanExpression(querySession, parent)

  override def toSql: String = s"${left.toSql} AND ${right.toSql}"
}

abstract class ValueExpression2(
  querySession: QuerySession,
  parent: Option[TreeNode] = None)
  extends TreeNode(querySession, parent) {

  querySession.nestedExpressionCount += 1
}

object ValueExpression2 {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): ValueExpression2 = {

    val maxNestedCount = 1

    val random = new Random()

    random.nextInt(3) match {
      case 0 if querySession.nestedExpressionCount <= maxNestedCount =>
        new ArithmeticUnary(querySession, parent)
      case 1 if querySession.nestedExpressionCount <= maxNestedCount =>
        new Comparison(querySession, parent)
      case _ =>
        PrimaryExpression2(querySession, parent)
    }
  }
}

class ArithmeticUnary(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends ValueExpression2(querySession, parent) {

  private val operators = Array("-", "+", "~")
  val operator = operators(random.nextInt(operators.length))
  val valueExpression = ValueExpression2(querySession, parent)
  override def toSql: String = s"$operator ${valueExpression.toSql}"
}

class Comparison(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends ValueExpression2(querySession, parent) {

  private val comparators = Array("==", "<>", "!=", "<", "<=", ">", ">=", "<=>")
  val comparator = comparators(random.nextInt(comparators.length))
  val left = ValueExpression2(querySession, parent)
  val right = ValueExpression2(querySession, parent)
  override def toSql: String = s"${left.toSql} $comparator ${right.toSql}"
}

abstract class PrimaryExpression2(
  querySession: QuerySession,
  parent: Option[TreeNode] = None)
  extends ValueExpression2(querySession, parent) {

  querySession.nestedExpressionCount += 1
}

object PrimaryExpression2 {
  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode] = None): PrimaryExpression2 = {

    val random = new Random()

    random.nextInt(3) match {
      case 0 => new ConstantDefault(querySession, parent)
      case 1 => new Star(querySession, parent)
      case 2 => new ColumnReference(querySession, parent)
    }
  }
}

class ConstantDefault(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends PrimaryExpression2(querySession, parent) {

  val valueGenerator = new ValueGenerator(random)
  val dataType = querySession.allowedDataTypes(random.nextInt(querySession.allowedDataTypes.length))
  val constant: Any = valueGenerator.generateValue(dataType)
  override def toSql: String = constant.toString
}

class Star(
  querySession: QuerySession,
  parent: Option[TreeNode] = None)
  extends PrimaryExpression2(querySession, parent) {
  override def toSql: String = "*"
}

class ColumnReference(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends PrimaryExpression2(querySession, parent) {

  override def toSql: String = "column_1"
}

abstract class Predicate(
  querySession: QuerySession,
  parent: Option[TreeNode] = None)
  extends TreeNode(querySession, parent) {

  val notOption: Option[String] = if (random.nextBoolean()) Some("NOT") else None
}

object Predicate {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode] = None): Predicate = {

    val random = new Random()
    random.nextInt(4) match {
      case 0 => new BetweenPredicate(querySession, parent)
      case 1 => new InPredicate(querySession, parent)
      case 2 => new LikePredicate(querySession, parent)
      case 3 => new NullPredicate(querySession, parent)
    }
  }
}

class BetweenPredicate(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends Predicate(querySession, parent) {

  val left = ValueExpression2(querySession, parent)
  val right = ValueExpression2(querySession, parent)

  override def toSql: String = s"${notOption.map(" " + _).getOrElse("")}" +
    s" BETWEEN ${left.toSql} AND ${right.toSql}"
}

class InPredicate(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends Predicate(querySession, parent) {

  // TODO: expressionSeq with at least one element
  val expression = BooleanExpression(querySession, parent)
  override def toSql: String = s"${notOption.map(" " + _).getOrElse("")}" +
    s" IN (${expression.toSql})"
}

class LikePredicate(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends Predicate(querySession, parent) {

  val valueExpression = ValueExpression2(querySession, parent)
  override def toSql: String = s"${notOption.map(" " + _).getOrElse("")}" +
    s" LIKE ${valueExpression.toSql}"
}

class NullPredicate(
    querySession: QuerySession,
    parent: Option[TreeNode] = None)
  extends Predicate(querySession, parent) {

  override def toSql: String = s" IS${notOption.map(" " + _).getOrElse("")} NULL"
}