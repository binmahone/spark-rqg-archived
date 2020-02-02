package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RandomUtils

trait Predicate extends TreeNode {
  def name: String
}

object Predicate {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): Predicate = {

    val choices = Array("NullPredicate")

    RandomUtils.choice(choices) match {
      case "NullPredicate" => NullPredicate(querySession, parent)
    }
  }
}
