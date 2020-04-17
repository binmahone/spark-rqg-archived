package org.apache.spark.rqg.ast.expressions

import java.text.DecimalFormat

import org.apache.spark.rqg._
import org.apache.spark.rqg.ast.{AggPreference, ExpressionGenerator, Function, QueryContext, Signature, TreeNode}
import org.apache.spark.rqg.ast.functions._

/**
 * primaryExpression
 *      : name=(CURRENT_DATE | CURRENT_TIMESTAMP)                                                  #currentDatetime
 *      | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
 *      | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
 *      | CAST '(' expression AS dataType ')'                                                      #cast
 *      | STRUCT '(' (argument+=namedExpression (',' argument+=namedExpression)*)? ')'             #struct
 *      | (FIRST | FIRST_VALUE) '(' expression ((IGNORE | RESPECT) NULLS)? ')'                     #first
 *      | (LAST | LAST_VALUE) '(' expression ((IGNORE | RESPECT) NULLS)? ')'                       #last
 *      | POSITION '(' substr=valueExpression IN str=valueExpression ')'                           #position
 *      | constant                                                                                 #constantDefault
 *      | ASTERISK                                                                                 #star
 *      | qualifiedName '.' ASTERISK                                                               #star
 *      | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
 *      | '(' query ')'                                                                            #subqueryExpression
 *      | qualifiedName '(' (setQuantifier? argument+=expression (',' argument+=expression)*)? ')'
 *         (OVER windowSpec)?                                                                      #functionCall
 *      | IDENTIFIER '->' expression                                                               #lambda
 *      | '(' IDENTIFIER (',' IDENTIFIER)+ ')' '->' expression                                     #lambda
 *      | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
 *      | identifier                                                                               #columnReference
 *      | base=primaryExpression '.' fieldName=identifier                                          #dereference
 *      | '(' expression ')'                                                                       #parenthesizedExpression
 *      | EXTRACT '(' field=identifier FROM source=valueExpression ')'                             #extract
 *      | (SUBSTR | SUBSTRING) '(' str=valueExpression (FROM | ',') pos=valueExpression
 *        ((FOR | ',') len=valueExpression)? ')'                                                   #substring
 *      | TRIM '(' trimOption=(BOTH | LEADING | TRAILING)? (trimStr=valueExpression)?
 *         FROM srcStr=valueExpression ')'                                                         #trim
 *      | OVERLAY '(' input=valueExpression PLACING replace=valueExpression
 *        FROM position=valueExpression (FOR length=valueExpression)? ')'                          #overlay
 *
 * This is the class that generate concrete Expressions
 * For now, we only support column, constant, start, functionCall
 */
trait PrimaryExpression extends ValueExpression

/**
 * It random generate one class extends PrimaryExpression
 */
object PrimaryExpression extends ExpressionGenerator[PrimaryExpression] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): PrimaryExpression = {

    val filteredChoices = (if (querySession.needGenerateAggFunction) {
      choices.filter(_.canGenerateAggFunc)
    } else if (querySession.needGenerateColumnExpression) {
      choices.filter(_ == ColumnReference)
    } else if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }).filter(_.possibleDataTypes(querySession).exists(requiredDataType.acceptsType))

    RandomUtils.nextChoice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    choices.flatMap(_.possibleDataTypes(querySession)).distinct
  }

  def choices = Array(Constant, ColumnReference, Star, FunctionCall)

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = true
}

/**
 * Constant
 */
class Constant(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  val value: Any = generateValue

  private def generateValue = {
    RandomUtils.nextConstant(requiredDataType)
  }

  override def sql: String = requiredDataType match {
    case StringType | DateType | TimestampType => s"'${value.toString}'"
    case _ => value.toString
  }

  override def name: String = "constant"

  override def dataType: DataType[_] = requiredDataType

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = Seq.empty

  override def nonAggColumns: Seq[ColumnReference] = Seq.empty
}

/**
 * Constant generator
 */
object Constant extends ExpressionGenerator[Constant] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Constant = {
    new Constant(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    querySession.allowedDataTypes
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

class Star(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  override def sql: String = "*"

  override def name: String = "star"

  // TODO: what should we return?
  override def dataType: DataType[_] = ???

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = ???

  override def nonAggColumns: Seq[ColumnReference] = ???
}

/**
 * Star generator. This class is special since it has more than one data type
 */
object Star extends ExpressionGenerator[Star] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Star = {
    new Star(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array.empty
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

/**
 * grammar: qualifiedName '(' (setQuantifier? argument+=expression (',' argument+=expression)*)? ')'
 * All spark function call generated from here.
 */
class FunctionCall(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends PrimaryExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val func: Function = generateFunction

  val signature: Signature = generateSignature

  val arguments: Seq[BooleanExpression] = generateArguments

  private def generateFunction: Function = {
    val functions = if (queryContext.aggPreference == AggPreference.FORBID) {
      FunctionCall.supportedFunctions.filterNot(_.isAgg)
    } else if (queryContext.needGenerateAggFunction) {
      FunctionCall.supportedFunctions.filter(_.isAgg)
    } else {
      FunctionCall.supportedFunctions
    }
    RandomUtils.nextChoice(
      functions.filter(_.signatures.exists(s => requiredDataType.acceptsType(s.returnType))))
  }

  private def generateSignature: Signature = {
    RandomUtils.nextChoice(
      func.signatures.filter(s => requiredDataType.acceptsType(s.returnType)).toArray)
  }

  private def generateArguments: Seq[BooleanExpression] = {
    val previous = queryContext.aggPreference
    if (func.isAgg) queryContext.aggPreference = AggPreference.FORBID
    val length = signature.inputTypes.length
    val arguments = signature.inputTypes.zipWithIndex.map {
      case (dt, idx) =>
        BooleanExpression(queryContext, Some(this), dt, isLast = idx == (length - 1))
    }
    if (previous != AggPreference.FORBID) queryContext.aggPreference = AggPreference.ALLOW
    arguments
  }

  override def sql: String = s"${func.name}(${arguments.map(_.sql).mkString(", ")})"

  override def name: String = dataType match {
    case _: DecimalType => "func_decimal"
    case _ => s"func_${dataType.typeName}"
  }

  override def dataType: DataType[_] = signature.returnType

  override def isAgg: Boolean = func.isAgg || arguments.exists(_.isAgg)

  override def columns: Seq[ColumnReference] = arguments.flatMap(_.columns)

  override def nonAggColumns: Seq[ColumnReference] = if (isAgg) Seq.empty else columns
}

/**
 * FunctionCall generator
 */
object FunctionCall extends ExpressionGenerator[FunctionCall] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): FunctionCall = {
    new FunctionCall(querySession, parent, requiredDataType, isLast)
  }

  private def supportedFunctions = Array(COUNT, SUM, ABS, FIRST)

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    (if (querySession.aggPreference == AggPreference.FORBID) {
      supportedFunctions.filterNot(_.isAgg)
    } else {
      supportedFunctions
    }).flatMap(_.signatures).map(_.returnType).distinct
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = true
}

/**
 * Random pick a column from available relations
 */
class ColumnReference(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  private val relation = generateRelation
  private val column = generateColumn

  private def generateRelation = {
    if (queryContext.needColumnFromJoiningRelation) {
      queryContext.joiningRelation.getOrElse {
        throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
      }
    } else {
      RandomUtils.nextChoice(
        queryContext.availableRelations
          .filter(_.columns.exists(c => requiredDataType.acceptsType(c.dataType))))
    }
  }

  private def generateColumn = {
    val columns = relation.columns.filter(c => requiredDataType.acceptsType(c.dataType))
    RandomUtils.nextChoice(columns)
  }

  override def sql: String = s"${relation.name}.${column.name}"

  override def name: String = s"${relation.name}_${column.name}"

  override def dataType: DataType[_] = column.dataType

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = Seq(this)

  override def nonAggColumns: Seq[ColumnReference] = columns
}

/**
 * ColumnReference generator
 */
object ColumnReference extends ExpressionGenerator[ColumnReference] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ColumnReference = {
    new ColumnReference(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    querySession.dataTypesInAvailableRelations
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

