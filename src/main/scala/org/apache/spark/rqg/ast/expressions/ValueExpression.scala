package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg._
import org.apache.spark.rqg.ast.{ExpressionGenerator, Operator, QueryContext, TreeNode}
import org.apache.spark.rqg.ast.operators._

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
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): ValueExpression = {

    val filteredChoices = (if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else if (querySession.needGenerateRelationalExpression) {
      choices.filter(_.canGenerateRelational)
    } else if (querySession.needGenerateAggFunction) {
      choices.filter(_.canGenerateAggFunc)
    } else if (querySession.allowedNestedExpressionCount > 0 && isLast) {
      choices.filter(_.canGenerateNested)
    } else {
      choices
    }).filter(_.possibleDataTypes(querySession).exists(requiredDataType.acceptsType))
    RandomUtils.nextChoice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    (PrimaryExpression.possibleDataTypes(querySession) :+ BooleanType).distinct
  }

  def choices = Array(PrimaryExpression, ArithmeticUnary, ArithmeticBinary, Comparison)

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = true
}

/**
 * grammar: operator=(MINUS | PLUS | TILDE) valueExpression
 *
 * Here we assume its data type is always same to valueExpression's
 */
class ArithmeticUnary(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val operator: Operator = RandomUtils.nextChoice(operators)
  val valueExpression: ValueExpression = generateValueExpression

  private def generateValueExpression = {
    ValueExpression(queryContext, Some(this), requiredDataType, isLast)
  }
  private def operators = {
    requiredDataType match {
      case _: IntegralType[_] => Array(MINUS, PLUS, TILDE)
      case _ => Array(MINUS, PLUS)
    }
  }

  override def sql: String = s"${operator.op} (${valueExpression.sql})"

  override def name: String = s"${operator.name}_${valueExpression.name}"

  override def dataType: DataType[_] = valueExpression.dataType

  override def isAgg: Boolean = valueExpression.isAgg

  override def columns: Seq[ColumnReference] = valueExpression.columns

  override def nonAggColumns: Seq[ColumnReference] = valueExpression.nonAggColumns
}

/**
 * ArithmeticUnary generator
 */
object ArithmeticUnary extends ExpressionGenerator[ArithmeticUnary] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ArithmeticUnary =
    new ArithmeticUnary(querySession, parent, requiredDataType, isLast)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    ValueExpression.possibleDataTypes(querySession).filter(_.isInstanceOf[NumericType[_]])
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false
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
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends ValueExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val operator: Operator = RandomUtils.nextChoice(operators)
  val left: ValueExpression = generateLeft
  val right: ValueExpression = generateRight

  private def generateLeft = {
    ValueExpression(queryContext, Some(this), requiredDataType)
  }

  private def generateRight = {
    ValueExpression(queryContext, Some(this), requiredDataType, isLast)
  }

  private def operators = {
    if (requiredDataType.isInstanceOf[NumericType[_]]) {
      Array(MINUS, PLUS)
    } else if (!requiredDataType.isInstanceOf[MapType] && !requiredDataType.isInstanceOf[StructType]){
      Array(CONCAT)
    } else {
      throw RQGEmptyChoiceException("Do not have an operator that can support the required data type")
    }
  }

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def name: String = s"${left.name}_${operator.name}_${right.name}"

  override def dataType: DataType[_] = requiredDataType

  override def isAgg: Boolean = left.isAgg || right.isAgg

  override def columns: Seq[ColumnReference] = left.columns ++ right.columns

  override def nonAggColumns: Seq[ColumnReference] = left.nonAggColumns ++ right.nonAggColumns
}

/**
 * ArithmeticBinary generator
 *
 * Here we support numeric type(+/-) and string type(||)
 */
object ArithmeticBinary extends ExpressionGenerator[ArithmeticBinary] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean):  ArithmeticBinary = {
    new ArithmeticBinary(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    ValueExpression.possibleDataTypes(querySession).filterNot(_ == BooleanType)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false
}

/**
 * grammar: left=valueExpression comparisonOperator right=valueExpression
 *
 * Comparison is very important for JoinCriteria to generate relationship between left and right
 * table. Also, other clause may also need to create Comparison but with rule. So we need take
 * special care of this node's creation.
 */
class Comparison(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    isLast: Boolean) extends ValueExpression {

  queryContext.allowedNestedExpressionCount -= 1
  queryContext.requiredRelationalExpressionCount -= 1

  val valueDataType: DataType[_] = chooseDataType
  val operator: Operator = RandomUtils.nextChoice(operators)
  val left: ValueExpression = generateLeft
  val right: ValueExpression = generateRight

  // restore querySession back
  queryContext.requiredColumnCount = 0
  queryContext.needColumnFromJoiningRelation = false

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  private def chooseDataType = {
    queryContext.allowedDataTypes = DataType.joinableDataTypes
    val dataType = RandomUtils.nextChoice(queryContext.commonDataTypesForJoin)
    queryContext.allowedDataTypes = DataType.supportedDataTypes
    dataType
  }

  private def generateLeft: ValueExpression = {
    // For joinCriteria, we treat left and right expression as an independent new expression, and
    // use a new querySession state to control the generation. after this, we restore the state back
    // This is a little bit tricky but useful to make sure we have at least one column in the
    // child expression
    if (queryContext.joiningRelation.isDefined) {
      val previousNestedCount = queryContext.allowedNestedExpressionCount
      val nestedCount = RandomUtils.choice(0, previousNestedCount)
      queryContext.requiredColumnCount = 1
      queryContext.needColumnFromJoiningRelation = false
      queryContext.allowedNestedExpressionCount = nestedCount
      val expression = ValueExpression(queryContext, Some(this), valueDataType, isLast = true)
      // restore back
      queryContext.requiredColumnCount = 0
      queryContext.allowedNestedExpressionCount = previousNestedCount - nestedCount
      expression
    } else {
      ValueExpression(queryContext, Some(this), valueDataType)
    }
  }

  private def generateRight: ValueExpression = {
    // We always choose column from joining relation for right expr of comparison
    if (queryContext.joiningRelation.isDefined) {
      val previousNestedCount = queryContext.allowedNestedExpressionCount
      val nestedCount = RandomUtils.choice(0, previousNestedCount)
      queryContext.requiredColumnCount = 1
      queryContext.needColumnFromJoiningRelation = true
      queryContext.allowedNestedExpressionCount = nestedCount
      val expression = ValueExpression(queryContext, Some(this), valueDataType, isLast = true)
      // restore back
      queryContext.requiredColumnCount = 0
      queryContext.needColumnFromJoiningRelation = true
      queryContext.allowedNestedExpressionCount = previousNestedCount - nestedCount
      expression
    } else {
      ValueExpression(queryContext, Some(this), valueDataType, isLast)
    }
  }

  private def operators = Array(EQ, NEQ, NEQJ, LT, LTE, GT, GTE, NSEQ)

  override def name: String = s"${left.name}_${operator.name}_${right.name}"

  override def dataType: DataType[_] = BooleanType

  override def isAgg: Boolean = left.isAgg || right.isAgg

  override def columns: Seq[ColumnReference] = left.columns ++ right.columns

  override def nonAggColumns: Seq[ColumnReference] = left.nonAggColumns ++ right.nonAggColumns
}

/**
 * Comparison generator
 */
object Comparison extends ExpressionGenerator[Comparison] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Comparison = {

    require(requiredDataType == BooleanType, "Comparison can only return BooleanType")

    new Comparison(querySession, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array(BooleanType)
  }

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false
}
