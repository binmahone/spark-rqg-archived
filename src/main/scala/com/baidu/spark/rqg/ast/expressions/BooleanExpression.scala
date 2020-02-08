package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.{BooleanType, DataType, RandomUtils}
import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.operators._

/**
 * booleanExpression
 *   : NOT booleanExpression                                        #logicalNot
 *   | EXISTS '(' query ')'                                         #exists
 *   | valueExpression predicate?                                   #predicated
 *   | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
 *   | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
 *   ;
 *
 * The root class of all expressions defined in sqlbase.g4
 *
 * Here the name may lead some ambiguous: BooleanExpression will also generate non-boolean
 * expression.
 * for example: booleanExpression -> valueExpression -> primaryExpression -> constant
 */
trait BooleanExpression extends TreeNode with Expression

/**
 * BooleanExpression Generator. It random generate one class extends BooleanExpression
 */
object BooleanExpression extends ExpressionGenerator[BooleanExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): BooleanExpression = {
    RandomUtils.choice(choices).apply(querySession, parent, requiredDataType, isLast)
  }

  private def choices = Array(LogicalNot, Predicated, LogicalBinary)

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}

/**
 * grammar: NOT booleanExpression
 */
class LogicalNot(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    isLast: Boolean) extends BooleanExpression {

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    BooleanExpression(querySession, Some(this), BooleanType, isLast)
  }

  override def name: String = s"not_${booleanExpression.name}"

  override def sql: String = s"NOT (${booleanExpression.sql})"

  override def dataType: DataType[_] = BooleanType
}

/**
 * LogicalNot generator
 */
object LogicalNot extends ExpressionGenerator[LogicalNot] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): LogicalNot = {

    require(requiredDataType == BooleanType, "LogicalNot can only return BooleanType")
    new LogicalNot(querySession, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateNested: Boolean = true
}

/**
 * grammar1: left=booleanExpression operator=AND right=booleanExpression
 * grammar2: left=booleanExpression operator=OR right=booleanExpression
 *
 * Here we combine 2 grammar in one class
 */
class LogicalBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {

  require(requiredDataType == BooleanType, "LogicalBinary can only return BooleanType")

  val operator: Operator = RandomUtils.choice(operators)
  val left: BooleanExpression = generateLeft
  val right: BooleanExpression = generateRight

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def dataType: DataType[_] = BooleanType

  private def generateLeft: BooleanExpression = {
    BooleanExpression(querySession, Some(this), BooleanType)
  }

  private def generateRight: BooleanExpression = {
    BooleanExpression(querySession, Some(this), BooleanType, isLast)
  }

  private def operators = Array(AND, OR)

  override def name: String = s"${left.name}_${operator.name}_${right.name}"
}

/**
 * LogicalBinary generator
 */
object LogicalBinary extends ExpressionGenerator[LogicalBinary] {
  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean): LogicalBinary = {
    new LogicalBinary(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

/**
 * grammar: valueExpression predicate?
 *
 * Here we have a *predicate?* which means predicate can be None. Hence, `Predicated` can return
 * non-boolean data type. This is one important point during nested expression creation.
 */
class Predicated(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {

  val valueExpression: ValueExpression = generateValueExpression
  val predicateOption: Option[Predicate] = generatePredicateOption

  private def generateValueExpression = {
    ValueExpression(querySession, Some(this), requiredDataType, isLast)
  }

  private def generatePredicateOption = {
    if (RandomUtils.nextBoolean()) {
      Some(Predicate(querySession, Some(this), requiredDataType))
    } else {
      None
    }
  }
  override def sql: String = s"(${valueExpression.sql}) ${predicateOption.map(_.sql).getOrElse("")}"

  override def dataType: DataType[_] = if (predicateOption.isDefined) {
    BooleanType
  } else {
    valueExpression.dataType
  }

  override def name: String = s"${valueExpression.name}${predicateOption.map("_" + _).getOrElse("")}"
}

/**
 * Predicated generator
 */
object Predicated extends ExpressionGenerator[Predicated] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Predicated = {
    new Predicated(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}
