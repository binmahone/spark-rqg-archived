package com.baidu.spark.rqg.ast

/**
 * All TreeNode is generated by an companion object class which extends one of generators.
 *
 * This `generator` class is used to make sure TreeNode generator has same apply function and
 * we can easily random choose a generator from an Array. For example:
 *
 * val choices = Array(ConstantGenerator, ColumnGenerator, ExpressionGenerator)
 * RandomUtil.choose(choices).apply(querySession, parent)
 */
trait Generator[T]

/**
 * Common Generator trait, most TreeNode generator extends this
 */
trait TreeNodeGenerator[T] extends Generator[T] {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): T
}