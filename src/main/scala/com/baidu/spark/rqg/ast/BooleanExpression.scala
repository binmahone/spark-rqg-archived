package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RandomUtils

trait BooleanExpression extends TreeNode {
  def name: String
}

object BooleanExpression {

  private val maxNestedExpressionCount = 1

  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode]): BooleanExpression = {

    val choices = Array("Predicated")

    RandomUtils.choice(choices) match {
      case "Predicated" => Predicated(querySession, parent)
    }
  }
}
