package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

trait Predicate extends TreeNode {
  def name: String
}

object Predicate {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): Predicate = {

    val choices = Array("NullPredicate", "BetweenPredicate")

    RandomUtils.choice(choices) match {
      case "NullPredicate" => NullPredicate(querySession, parent)
      case "BetweenPredicate" => BetweenPredicate(querySession, parent)
    }
  }
}
