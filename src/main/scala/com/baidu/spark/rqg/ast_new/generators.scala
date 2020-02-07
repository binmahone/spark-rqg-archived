package com.baidu.spark.rqg.ast_new

import com.baidu.spark.rqg.DataType

trait Generator[T]

trait TreeNodeGenerator[T] extends Generator[T] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): T
}

trait RelationPrimaryGenerator[T] extends Generator[T] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): T
}

trait ExpressionGenerator[T] extends Generator[T] {
  def canGeneratePrimitive: Boolean
  def canGenerateRelational: Boolean
  def canGenerateNested: Boolean
  def possibleDataTypes: Array[DataType[_]]
  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean): T
}

trait PredicateGenerator[T] extends Generator[T] {
  def apply(querySession: QuerySession, parent: Option[TreeNode], requiredDataType: DataType[_]): T
}