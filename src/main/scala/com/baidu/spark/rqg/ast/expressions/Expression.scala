package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.DataType

/**
 * A trait that all expressions should extends. such as:
 * BooleanExpression, ValueExpression, PrimaryExpression, etc.
 */
trait Expression {
  def name: String
  def dataType: DataType[_]
}
