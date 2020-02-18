package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.{DataType, RandomUtils}
import org.apache.spark.rqg.ast.{ExpressionGenerator, QuerySession, TreeNode}

/**
 * namedExpression
 *     : expression (AS? (name=errorCapturingIdentifier | identifierList))?
 *     ;
 */
class NamedExpression(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends Expression {

  val expression: BooleanExpression = generateExpression

  val alias = Some(querySession.nextAlias(expression.name))

  private def generateExpression: BooleanExpression = {
    BooleanExpression(querySession, parent, requiredDataType, isLast)
  }

  def name: String = alias.getOrElse(expression.name)

  def sql: String = s"${expression.sql} ${alias.map("AS " + _).getOrElse("")}"

  override def dataType: DataType[_] = requiredDataType

  override def isAgg: Boolean = expression.isAgg

  override def columns: Seq[ColumnReference] = expression.columns

  override def nonAggColumns: Seq[ColumnReference] = expression.nonAggColumns
}

object NamedExpression extends ExpressionGenerator[NamedExpression] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredType: DataType[_],
      isLast: Boolean): NamedExpression = {
    new NamedExpression(querySession, parent, requiredType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false

  override def possibleDataTypes(querySession: QuerySession): Array[DataType[_]] =
    DataType.supportedDataTypes
}

