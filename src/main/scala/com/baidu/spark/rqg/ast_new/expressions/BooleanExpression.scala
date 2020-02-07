package com.baidu.spark.rqg.ast_new.expressions

import com.baidu.spark.rqg.ast_new.{ExpressionGenerator, PredicateGenerator, QuerySession, TreeNode}
import com.baidu.spark.rqg._
import com.baidu.spark.rqg.ast_new.expressions.operators._

trait Expression {
  def dataType: DataType[_]
}

trait BooleanExpression extends TreeNode with Expression

object BooleanExpression extends ExpressionGenerator[BooleanExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): BooleanExpression = {

    val filteredChoices = (if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else if (querySession.needGenerateRelationalExpression) {
      choices.filter(_.canGenerateRelational)
    } else if (querySession.allowedNestedExpressionCount > 0 && isLast) {
      choices.filter(_.canGenerateNested)
    } else {
      choices
    }).filter(_.possibleDataTypes.contains(requiredDataType))
    RandomUtils.choice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  def choices = Array(LogicalNot, Predicated, LogicalBinary)

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}

class LogicalNot(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {
  querySession.allowedNestedExpressionCount -= 1

  val booleanExpression: BooleanExpression =
    BooleanExpression(querySession, Some(this), BooleanType, isLast)
  override def sql: String = s"NOT (${booleanExpression.sql})"

  override def dataType: DataType[_] = BooleanType
}

object LogicalNot extends ExpressionGenerator[LogicalNot] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): LogicalNot =
    new LogicalNot(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = false

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateNested: Boolean = true
}

class Predicated(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {

  private val usePredicate =
    !querySession.needGeneratePrimitiveExpression &&
      !querySession.needGenerateRelationalExpression &&
      requiredDataType == BooleanType &&
      RandomUtils.nextBoolean()

  if (usePredicate) {
    querySession.allowedNestedExpressionCount -= 1
  }

  private val valueExpressionDataType = if (usePredicate) {
    val dataTypes = querySession.allowedDataTypes.intersect(querySession.availableRelations.flatMap(_.columns).map(_.dataType)).distinct
    RandomUtils.choice(dataTypes)
  } else {
    requiredDataType
  }
  val valueExpression: ValueExpression =
    ValueExpression(querySession, Some(this), requiredDataType, isLast)
  val predicateOption: Option[Predicate] = if (usePredicate) {
    Some(Predicate(querySession, Some(this), requiredDataType))
  } else {
    None
  }
  override def sql: String = s"(${valueExpression.sql}) ${predicateOption.map(_.sql).getOrElse("")}"

  override def dataType: DataType[_] = if (predicateOption.isDefined) {
    BooleanType
  } else {
    valueExpressionDataType
  }
}

object Predicated extends ExpressionGenerator[Predicated] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Predicated =
    new Predicated(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}

class LogicalBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {

  require(requiredDataType == BooleanType, "LogicalBinary can only return BooleanType")
  querySession.allowedNestedExpressionCount -= 1
  private val ops = Array(AND, OR)
  val operator: Operator = RandomUtils.choice(ops)
  val left: BooleanExpression =
    BooleanExpression(querySession, Some(this), BooleanType)
  val right: BooleanExpression =
    BooleanExpression(querySession, Some(this), BooleanType, isLast)
  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def dataType: DataType[_] = BooleanType
}

object LogicalBinary extends ExpressionGenerator[LogicalBinary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): LogicalBinary =
    new LogicalBinary(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

trait ValueExpression extends TreeNode

object ValueExpression extends ExpressionGenerator[ValueExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): ValueExpression = {
    val filteredChoices = (if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else if (querySession.needGenerateRelationalExpression) {
      choices.filter(_.canGenerateRelational)
    } else if (querySession.allowedNestedExpressionCount > 0 && isLast) {
      choices.filter(_.canGenerateNested)
    } else {
      choices
    }).filter(_.possibleDataTypes.contains(requiredDataType))
    RandomUtils.choice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  def choices = Array(PrimaryExpression, ArithmeticUnary, ArithmeticBinary, Comparison)

  override def canGenerateNested: Boolean = true
}

trait PrimaryExpression extends ValueExpression

object PrimaryExpression extends ExpressionGenerator[PrimaryExpression] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): PrimaryExpression = {
    val filteredChoices = (if (querySession.needGenerateColumnExpression) {
      choices.filter(_ == ColumnReference)
    } else if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }).filter(_.possibleDataTypes.contains(requiredDataType))
    RandomUtils.choice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  def choices = Array(Constant, ColumnReference, Star, FunctionCall)

  override def canGenerateNested: Boolean = false
}

class ArithmeticUnary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {
  querySession.allowedNestedExpressionCount -= 1
  private val ops = requiredDataType match {
    case _: IntegralType[_] => Array(MINUS, PLUS, TILDE)
    case _ => Array(MINUS, PLUS)
  }
  val operator: Operator = RandomUtils.choice(ops)
  val valueExpression: ValueExpression =
    ValueExpression(querySession, Some(this), requiredDataType, isLast)

  override def sql: String = s"${operator.op} (${valueExpression.sql})"
}

object ArithmeticUnary extends ExpressionGenerator[ArithmeticUnary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ArithmeticUnary =
    new ArithmeticUnary(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] =
    DataType.supportedDataTypes.filter(x => x.isInstanceOf[NumericType[_]])

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

class ArithmeticBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  querySession.allowedNestedExpressionCount -= 1
  // private val ops = Array(MINUS, PLUS, MULTIPLY, DIVIDE, MOD)
  private val ops = if (requiredDataType.isInstanceOf[NumericType[_]]) {
    Array(MINUS, PLUS)
  } else {
    Array(CONCAT)
  }
  val operator: Operator = RandomUtils.choice(ops)
  val left: ValueExpression = ValueExpression(querySession, Some(this), requiredDataType)
  val right: ValueExpression = ValueExpression(querySession, Some(this), requiredDataType, isLast)
  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"
}

object ArithmeticBinary extends ExpressionGenerator[ArithmeticBinary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean):  ArithmeticBinary =
    new ArithmeticBinary(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] =
    DataType.supportedDataTypes.filterNot(_ == BooleanType)

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

class Comparison(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  require(requiredDataType == BooleanType, "Comparison can only return BooleanType")

  querySession.allowedNestedExpressionCount -= 1
  querySession.requiredRelationalExpressionCount -= 1

  val dataType: DataType[_] = chooseDataType
  val operator: Operator = chooseOperator
  val left: ValueExpression = generateLeft
  val right: ValueExpression = generateRight

  // restore this back
  querySession.requiredColumnCount = 0
  querySession.needColumnFromJoiningRelation = false

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  // TODO: actually, we need choose data type that child expression supports
  // For example, if ValueExpression can't generate string expression, we should skip it.
  private def chooseDataType: DataType[_] = {
    val allowedDataTypes = querySession.allowedDataTypes
    val availableDataTypes = querySession.availableRelations.flatMap(_.columns).map(_.dataType)
    val joiningDataTypes = querySession.joiningRelation.map(_.columns.map(_.dataType))
    val dataTypes = joiningDataTypes
      .map(_.intersect(allowedDataTypes.intersect(availableDataTypes)))
      .getOrElse(allowedDataTypes.intersect(availableDataTypes))
    RandomUtils.choice(dataTypes)
  }

  private def chooseOperator: Operator = {
    val ops = Array(EQ, NEQ, NEQJ, LT, LTE, GT, GTE, NSEQ)
    RandomUtils.choice(ops)
  }

  private def generateLeft: ValueExpression = {
    if (querySession.joiningRelation.isDefined) {
      querySession.requiredColumnCount = 1
      querySession.needColumnFromJoiningRelation = false
    }
    ValueExpression(querySession, Some(this), dataType)
  }

  private def generateRight: ValueExpression = {
    // We always choose column from joining relation for right expr of comparison
    if (querySession.joiningRelation.isDefined) {
      querySession.requiredColumnCount = 1
      querySession.needColumnFromJoiningRelation = true
    }
    ValueExpression(querySession, Some(this), dataType, isLast)
  }
}

object Comparison extends ExpressionGenerator[Comparison] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Comparison = {
    new Comparison(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}

trait Predicate extends TreeNode {
  val notOption: Option[String] = if (RandomUtils.nextBoolean()) {
    Some("NOT")
  } else {
    None
  }
}

object Predicate extends PredicateGenerator[Predicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): Predicate = {

    val choices = Array(BetweenPredicate, InPredicate, LikePredicate, NullPredicate)
    val choice = RandomUtils.choice(choices)
    choice.apply(querySession, parent, requiredDataType)
  }
}

class BetweenPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val lower: ValueExpression = ValueExpression(querySession, Some(this), requiredDataType)
  val upper: ValueExpression = ValueExpression(querySession, Some(this), requiredDataType)
  override def sql: String = s"${notOption.getOrElse("")} BETWEEN ${lower.sql} AND ${upper.sql}"
}

object BetweenPredicate extends PredicateGenerator[BetweenPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): BetweenPredicate = {
    new BetweenPredicate(querySession, parent, requiredDataType)
  }
}

class InPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val expressionSeq: Seq[BooleanExpression] =
    Seq(BooleanExpression(querySession, Some(this), requiredDataType))
  override def sql: String =
    s"${notOption.getOrElse("")} IN (${expressionSeq.map(_.sql).mkString(", ")})"
}

object InPredicate extends PredicateGenerator[InPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): InPredicate = {
    new InPredicate(querySession, parent, requiredDataType)
  }
}

class LikePredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val valueExpression = ValueExpression(querySession, Some(this), requiredDataType)
  override def sql: String = s"${notOption.getOrElse("")} LIKE ${valueExpression.sql}"
}

object LikePredicate extends PredicateGenerator[LikePredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): LikePredicate = {
    new LikePredicate(querySession, parent, requiredDataType)
  }
}

class NullPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {
  override def sql: String = s"IS ${notOption.getOrElse("")} NULL"
}

object NullPredicate extends PredicateGenerator[NullPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): NullPredicate = {
    new NullPredicate(querySession, parent, requiredDataType)
  }
}

class Constant(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  val value: Any = RandomUtils.nextConstant(requiredDataType)
  override def sql: String = requiredDataType match {
    case _: StringType => s"'${value.toString}'"
    case _ => value.toString
  }
}

object Constant extends ExpressionGenerator[Constant] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Constant = {
    new Constant(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

class Star(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  override def sql: String = "*"
}

object Star extends ExpressionGenerator[Star] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Star = {
    new Star(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = Array.empty

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

class FunctionCall(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends PrimaryExpression {
  querySession.allowedNestedExpressionCount -= 1
  private val dataType = RandomUtils.choice(querySession.allowedDataTypes)
  override def sql: String = s"FunctionCall_$dataType"
}

object FunctionCall extends ExpressionGenerator[FunctionCall] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): FunctionCall = {
    new FunctionCall(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array.empty

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

class ColumnReference(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  if (querySession.requiredColumnCount > 0) querySession.requiredColumnCount -= 1

  private val relation = if (querySession.needColumnFromJoiningRelation) {
    querySession.joiningRelation.getOrElse {
      throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
    }
  } else {
    RandomUtils.choice(Utils.allowedRelations(querySession.availableRelations, requiredDataType))
  }
  private val column = RandomUtils.choice(relation.columns.filter(_.dataType == requiredDataType))
  override def sql: String = s"${relation.name}.${column.name}"
}

object ColumnReference extends ExpressionGenerator[ColumnReference] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ColumnReference = {
    new ColumnReference(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

case class Operator(name: String, op: String)

object operators {
  val AND = Operator("and", "AND")
  val OR = Operator("or", "OR")
  val MINUS = Operator("minus", "-")
  val PLUS = Operator("plus", "+")
  val CONCAT = Operator("concat", "||")
  val TILDE = Operator("tilde", "~")
  val DIVIDE = Operator("divide", "/")
  val MULTIPLY = Operator("multiply", "*")
  val MOD = Operator("mod", "%")
  val EQ = Operator("eq", "==")
  val NEQ = Operator("neq", "<>")
  val NEQJ = Operator("neqj", "!=")
  val LT = Operator("lt", "<")
  val LTE = Operator("lte", "<=")
  val GT = Operator("gt", ">")
  val GTE = Operator("gte", ">=")
  val NSEQ = Operator("nseq", "<=>")
}
