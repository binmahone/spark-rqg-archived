package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.{DataType, RandomUtils, StringType}
import com.baidu.spark.rqg.ast.{ExpressionGenerator, QuerySession, TreeNode}

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
    }).filter(_.possibleDataTypes(querySession).contains(requiredDataType))
    RandomUtils.choice(filteredChoices).apply(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] = {
    choices.flatMap(_.possibleDataTypes(querySession)).distinct
  }

  def choices = Array(Constant, ColumnReference, Star, FunctionCall)

  override def canGenerateNested: Boolean = false
}

/**
 * Constant
 */
class Constant(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  val value: Any = generateValue

  private def generateValue = {
    RandomUtils.nextConstant(requiredDataType)
  }

  override def sql: String = requiredDataType match {
    case _: StringType => s"'${value.toString}'"
    case _ => value.toString
  }

  override def name: String = "constant"

  override def dataType: DataType[_] = requiredDataType
}

/**
 * Constant generator
 */
object Constant extends ExpressionGenerator[Constant] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Constant = {
    new Constant(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] = {
    querySession.allowedDataTypes
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

class Star(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  override def sql: String = "*"

  override def name: String = "star"

  // TODO: what should we return?
  override def dataType: DataType[_] = ???
}

/**
 * Star generator. This class is special since it has more than one data type
 */
object Star extends ExpressionGenerator[Star] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Star = {
    new Star(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] = {
    Array.empty
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

/**
 * All spark function call generated from here.
 */
class FunctionCall(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends PrimaryExpression {

  override def sql: String = s"FunctionCall_$dataType"

  override def name: String = s"func_$dataType"

  override def dataType: DataType[_] = requiredDataType
}

/**
 * FunctionCall generator
 */
object FunctionCall extends ExpressionGenerator[FunctionCall] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): FunctionCall = {
    new FunctionCall(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] = {
    Array.empty
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true
}

/**
 * Random pick a column from available relations
 */
class ColumnReference(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  private val relation = generateRelation
  private val column = generateColumn

  private def generateRelation = {
    if (querySession.needColumnFromJoiningRelation) {
      querySession.joiningRelation.getOrElse {
        throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
      }
    } else {
      RandomUtils.choice(
        querySession.availableRelations
          .filter(_.columns.exists(_.dataType == requiredDataType)))
    }
  }

  private def generateColumn = {
    RandomUtils.choice(relation.columns.filter(_.dataType == requiredDataType))
  }

  override def sql: String = s"${relation.name}.${column.name}"

  override def name: String = s"${relation.name}_${column.name}"

  override def dataType: DataType[_] = requiredDataType
}

/**
 * ColumnReference generator
 */
object ColumnReference extends ExpressionGenerator[ColumnReference] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ColumnReference = {
    new ColumnReference(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] = {
    querySession.dataTypesInAvailableRelations
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false
}

