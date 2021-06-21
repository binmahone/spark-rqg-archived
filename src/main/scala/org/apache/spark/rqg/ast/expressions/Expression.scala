package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.DataType

/**
 * A trait that all expressions should extends. such as:
 * BooleanExpression, ValueExpression, PrimaryExpression, etc.
 */
trait Expression {
  def name: String
  def dataType: DataType[_]
  def isAgg: Boolean
  def sql: String

  def columns: Seq[ColumnReference]
  def nonAggColumns: Seq[ColumnReference]
}
