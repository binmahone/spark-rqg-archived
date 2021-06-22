package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.{DataType, RandomUtils}
import org.apache.spark.rqg.ast.{ExpressionGenerator, QueryContext, TreeNode}

/**
 * namedExpression
 *     : expression (AS? (name=errorCapturingIdentifier | identifierList))?
 *     ;
 */
class NamedExpression(
    val querySession: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends Expression {

  val expression: ValueExpression = generateExpression

  val alias = Some(querySession.nextAlias("expr"))

  private def generateExpression: ValueExpression = {
    ValueExpression(querySession, parent, requiredDataType, isLast)
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
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredType: DataType[_],
      isLast: Boolean): NamedExpression = {
    new NamedExpression(querySession, parent, requiredType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] =
    DataType.supportedDataTypes(querySession.rqgConfig)
}

