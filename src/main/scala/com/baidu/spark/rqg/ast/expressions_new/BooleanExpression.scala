package com.baidu.spark.rqg.ast.expressions_new

import com.baidu.spark.rqg.{BooleanType, DataType, RandomUtils, StringType}

trait Expression {
  def dataType: DataType[_]
}

trait BooleanExpression extends TreeNode with Expression

trait Generator[T] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): T
}

trait ExpressionGenerator[T] extends Generator[T] {
  def canGeneratePrimitive: Boolean
  def possibleDataTypes: Array[DataType[_]]
}
trait PredicateGenerator[T] extends Generator[T]

object BooleanExpression extends ExpressionGenerator[BooleanExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): BooleanExpression = {
    val choices = Array(LogicalNot, Predicated, LogicalBinary)
    val filteredChoices = if (querySession.allowedNestedExpressionCount <= 0) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }
    val filteredChoices2 = filteredChoices.filter { choice =>
      choice.possibleDataTypes.intersect(querySession.allowedDataTypes).nonEmpty
    }
    val choice = RandomUtils.choice(filteredChoices2)
    choice.apply(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

class LogicalNot(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends BooleanExpression {
  querySession.allowedNestedExpressionCount -= 1
  querySession.allowedDataTypes = Array(dataType)

  val booleanExpression: BooleanExpression = BooleanExpression(querySession, Some(this))
  override def sql: String = s"NOT (${booleanExpression.sql})"

  override def dataType: DataType[_] = BooleanType
}

object LogicalNot extends ExpressionGenerator[LogicalNot] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): LogicalNot =
    new LogicalNot(querySession, parent)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)
}

class Predicated(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends BooleanExpression {
  private val returnDataType = RandomUtils.choice(querySession.allowedDataTypes)

  private val usePredicate =
    querySession.allowedNestedExpressionCount > 0 && returnDataType == BooleanType && RandomUtils.nextBoolean()

  if (usePredicate) {
    querySession.allowedNestedExpressionCount -= 1
  }

  private val valueExpressionDataType = if (usePredicate) {
    RandomUtils.choice(DataType.supportedDataTypes)
  } else {
    returnDataType
  }
  querySession.allowedDataTypes = Array(valueExpressionDataType)
  val valueExpression: ValueExpression = ValueExpression(querySession, Some(this))
  val predicateOption: Option[Predicate] = if (usePredicate) {
    Some(Predicate(querySession, Some(this)))
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
  def apply(querySession: QuerySession, parent: Option[TreeNode]): Predicated =
    new Predicated(querySession, parent)

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

class LogicalBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends BooleanExpression {
  querySession.allowedNestedExpressionCount -= 1
  import operators._
  private val ops = Array(AND, OR)
  querySession.allowedDataTypes = Array(BooleanType)
  val operator: Operator = RandomUtils.choice(ops)
  val left: BooleanExpression = BooleanExpression(querySession, Some(this))
  querySession.allowedDataTypes = Array(BooleanType)
  val right: BooleanExpression = BooleanExpression(querySession, Some(this))
  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def dataType: DataType[_] = BooleanType
}

object LogicalBinary extends ExpressionGenerator[LogicalBinary] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): LogicalBinary =
    new LogicalBinary(querySession, parent)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)
}

trait ValueExpression extends TreeNode

object ValueExpression extends ExpressionGenerator[ValueExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): ValueExpression = {
    val choices = Array(PrimaryExpression, ArithmeticUnary, ArithmeticBinary, Comparison)
    val filteredChoices = if (querySession.allowedNestedExpressionCount <= 0) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }
    val filteredChoices2 = filteredChoices.filter { choice =>
      choice.possibleDataTypes.intersect(querySession.allowedDataTypes).nonEmpty
    }
    val choice = RandomUtils.choice(filteredChoices2)
    choice.apply(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

trait PrimaryExpression extends ValueExpression

object PrimaryExpression extends ExpressionGenerator[PrimaryExpression] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): PrimaryExpression = {
    val choices = Array(Constant, ColumnReference, Star, FunctionCall)
    val filteredChoices = if (querySession.allowedNestedExpressionCount <= 0) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }
    val filteredChoices2 = filteredChoices.filter { choice =>
      choice.possibleDataTypes.intersect(querySession.allowedDataTypes).nonEmpty
    }
    val choice = RandomUtils.choice(filteredChoices2)
    choice.apply(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

class ArithmeticUnary(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends ValueExpression {
  querySession.allowedNestedExpressionCount -= 1
  import operators._
  private val ops = Array(MINUS, PLUS, TILDE)
  private val expressionDataType = RandomUtils.choice(ArithmeticUnary.possibleDataTypes)
  querySession.allowedDataTypes = Array(expressionDataType)
  val operator: Operator = RandomUtils.choice(ops)
  val valueExpression: ValueExpression = ValueExpression(querySession, Some(this))

  override def sql: String = s"${operator.op} (${valueExpression.sql})"
}

object ArithmeticUnary extends ExpressionGenerator[ArithmeticUnary] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): ArithmeticUnary =
    new ArithmeticUnary(querySession, parent)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] =
    DataType.supportedDataTypes.filterNot(x => x == BooleanType || x == StringType())
}

class ArithmeticBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends ValueExpression {

  querySession.allowedNestedExpressionCount -= 1
  import operators._
  private val ops = Array(MINUS, PLUS, MULTIPLY, DIVIDE, MOD)
  private val expressionDataType = RandomUtils.choice(ArithmeticBinary.possibleDataTypes)
  querySession.allowedDataTypes = Array(expressionDataType)
  val operator: Operator = RandomUtils.choice(ops)
  val left: ValueExpression = ValueExpression(querySession, Some(this))
  querySession.allowedDataTypes = Array(expressionDataType)
  val right: ValueExpression = ValueExpression(querySession, Some(this))
  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"
}

object ArithmeticBinary extends ExpressionGenerator[ArithmeticBinary] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): ArithmeticBinary =
    new ArithmeticBinary(querySession, parent)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] =
    DataType.supportedDataTypes.filterNot(x => x == BooleanType || x == StringType())
}

class Comparison(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends ValueExpression {

  querySession.allowedNestedExpressionCount -= 1
  import operators._
  private val ops = Array(EQ, NEQ, NEQJ, LT, LTE, GT, GTE, NSEQ)
  private val dataType = RandomUtils.choice(DataType.supportedDataTypes)
  querySession.allowedDataTypes = Array(dataType)
  val operator: Operator = RandomUtils.choice(ops)
  val left: ValueExpression = ValueExpression(querySession, Some(this))
  querySession.allowedDataTypes = Array(dataType)
  val right: ValueExpression = ValueExpression(querySession, Some(this))
  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"
}

object Comparison extends ExpressionGenerator[Comparison] {
  override def apply(querySession: QuerySession, parent: Option[TreeNode]): Comparison = {
    new Comparison(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)
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
      parent: Option[TreeNode]): Predicate = {

    val choices = Array(BetweenPredicate, InPredicate, LikePredicate, NullPredicate)
    val choice = RandomUtils.choice(choices)
    choice.apply(querySession, parent)
  }
}

class BetweenPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends Predicate {

  val lower: ValueExpression = ValueExpression(querySession, Some(this))
  val upper: ValueExpression = ValueExpression(querySession, Some(this))
  override def sql: String = s"${notOption.getOrElse("")} BETWEEN ${lower.sql} AND ${upper.sql}"
}

object BetweenPredicate extends PredicateGenerator[BetweenPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): BetweenPredicate = {
    new BetweenPredicate(querySession, parent)
  }
}

class InPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends Predicate {

  val expressionSeq: Seq[BooleanExpression] = Seq(BooleanExpression(querySession, Some(this)))
  override def sql: String =
    s"${notOption.getOrElse("")} IN (${expressionSeq.map(_.sql).mkString(", ")})"
}

object InPredicate extends PredicateGenerator[InPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): InPredicate = {
    new InPredicate(querySession, parent)
  }
}

class LikePredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends Predicate {

  val valueExpression = ValueExpression(querySession, Some(this))
  override def sql: String = s"${notOption.getOrElse("")} LIKE ${valueExpression.sql}"
}

object LikePredicate extends PredicateGenerator[LikePredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): LikePredicate = {
    new LikePredicate(querySession, parent)
  }
}

class NullPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends Predicate {
  override def sql: String = s"IS ${notOption.getOrElse("")} NULL"
}

object NullPredicate extends PredicateGenerator[NullPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): NullPredicate = {
    new NullPredicate(querySession, parent)
  }
}

class Constant(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {

  val dataType: DataType[_] = RandomUtils.choice(querySession.allowedDataTypes)
  val value: Any = RandomUtils.nextConstant(dataType)
  override def sql: String = value.toString
}

object Constant extends ExpressionGenerator[Constant] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Constant = {
    new Constant(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

class Star(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  override def sql: String = "*"
}

object Star extends ExpressionGenerator[Star] {
  override def apply(
    querySession: QuerySession,
    parent: Option[TreeNode]): Star = {
    new Star(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = Array.empty
}

class FunctionCall(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  querySession.allowedNestedExpressionCount -= 1
  private val dataType = RandomUtils.choice(querySession.allowedDataTypes)
  override def sql: String = s"FunctionCall_$dataType"
}

object FunctionCall extends ExpressionGenerator[FunctionCall] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): FunctionCall = {
    new FunctionCall(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array.empty
}

class ColumnReference(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {

  private val dataType = RandomUtils.choice(querySession.allowedDataTypes)
  override def sql: String = s"column_$dataType"
}

object ColumnReference extends ExpressionGenerator[ColumnReference] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): ColumnReference = {
    new ColumnReference(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes
}

case class Operator(name: String, op: String)

object operators {
  val AND = Operator("and", "AND")
  val OR = Operator("or", "OR")
  val MINUS = Operator("minus", "-")
  val PLUS = Operator("plus", "+")
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
