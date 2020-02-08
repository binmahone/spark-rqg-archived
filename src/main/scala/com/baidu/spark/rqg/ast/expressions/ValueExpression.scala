package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.{BooleanType, DataType, NumericType, RandomUtils}
import com.baidu.spark.rqg.ast.{ExpressionGenerator, Operator, QuerySession, TreeNode}
import com.baidu.spark.rqg.ast.operators._

/**
 * valueExpression
 *     : primaryExpression                                                                      #valueExpressionDefault
 *     | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
 *     | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
 *     | left=valueExpression operator=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
 *     | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
 *     | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
 *     | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
 *     | left=valueExpression comparisonOperator right=valueExpression                          #comparison
 *     ;
 * another nested expression grammar similar to booleanExpression
 */
trait ValueExpression extends TreeNode with Expression

/**
 * ValueExpression Generator. It random generate one class extends ValueExpression
 */
object ValueExpression extends ExpressionGenerator[ValueExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): ValueExpression = {

    RandomUtils.choice(choices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def possibleDataTypes: Array[DataType[_]] = DataType.supportedDataTypes

  def choices = Array(PrimaryExpression, ArithmeticUnary, ArithmeticBinary, Comparison)

  override def canGenerateNested: Boolean = true
}

/**
 * grammar: operator=(MINUS | PLUS | TILDE) valueExpression
 *
 * Here we assume its data type is always same to valueExpression's
 */
class ArithmeticUnary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  val operator: Operator = RandomUtils.choice(operators)
  val valueExpression: ValueExpression = generateValueExpression

  private def generateValueExpression = {
    ValueExpression(querySession, Some(this), requiredDataType, isLast)
  }
  private def operators = Array(MINUS, PLUS, TILDE)

  override def sql: String = s"${operator.op} (${valueExpression.sql})"

  override def name: String = s"${operator.name}_${valueExpression.name}"

  override def dataType: DataType[_] = valueExpression.dataType
}

/**
 * ArithmeticUnary generator
 */
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

/**
 * grammars:
 * left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression
 * left=valueExpression operator=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression
 * left=valueExpression operator=AMPERSAND right=valueExpression
 * left=valueExpression operator=HAT right=valueExpression
 * left=valueExpression operator=PIPE right=valueExpression
 *
 * Different operator returns different data type, for now we only support PLUS / MINUS / CONCAT
 */
class ArithmeticBinary(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  val operator: Operator = RandomUtils.choice(operators)
  val left: ValueExpression = generateLeft
  val right: ValueExpression = generateRight

  private def generateLeft = {
    ValueExpression(querySession, Some(this), requiredDataType)
  }

  private def generateRight = {
    ValueExpression(querySession, Some(this), requiredDataType, isLast)
  }

  private def operators = {
    Array(MINUS, PLUS, CONCAT)
  }

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def name: String = s"${left.name}_${operator.name}}_${right.name}"

  override def dataType: DataType[_] = requiredDataType
}

/**
 * ArithmeticBinary generator
 *
 * Here we support numeric type(+/-) and string type(||)
 */
object ArithmeticBinary extends ExpressionGenerator[ArithmeticBinary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean):  ArithmeticBinary = {
    new ArithmeticBinary(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = {
    DataType.supportedDataTypes.filterNot(_ == BooleanType)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

/**
 * grammar: left=valueExpression comparisonOperator right=valueExpression
 *
 * Comparison is very important for JoinCriteria to generate relationship between left and right
 * table. Also, other clause may also need to create Comparison but with rule. So we need take
 * special care of this node's creation.
 */
class Comparison(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    isLast: Boolean) extends ValueExpression {

  val dataType: DataType[_] = RandomUtils.choice(querySession.allowedDataTypes)
  val operator: Operator = RandomUtils.choice(operators)
  val left: ValueExpression = generateLeft
  val right: ValueExpression = generateRight

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  private def generateLeft: ValueExpression = {
    ValueExpression(querySession, Some(this), dataType)
  }

  private def generateRight: ValueExpression = {
    ValueExpression(querySession, Some(this), dataType, isLast)
  }

  private def operators = Array(EQ, NEQ, NEQJ, LT, LTE, GT, GTE, NSEQ)

  override def name: String = s"${left.name}_${operator.name}_${right.name}"
}

/**
 * Comparison generator
 */
object Comparison extends ExpressionGenerator[Comparison] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Comparison = {

    require(requiredDataType == BooleanType, "Comparison can only return BooleanType")

    new Comparison(querySession, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes: Array[DataType[_]] = Array(BooleanType)

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true
}
