package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class NullPredicate(
    querySession: QuerySession,
    parent: Option[TreeNode],
    notOption: Option[String]) extends Predicate {

  override def sql: String = s"IS ${notOption.getOrElse("")} NULL"

  override def name: String = s"is_${notOption.getOrElse("")}_null"
}

object NullPredicate {

  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): NullPredicate = {

    val notOption = if (RandomUtils.nextBoolean()) Some("NOT") else None
    NullPredicate(querySession, parent, notOption)
  }
}
