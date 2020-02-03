package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast._

case class BetweenPredicate(
    querySession: QuerySession,
    parent: Option[TreeNode],
    notOption: Option[String],
    lower: ValueExpression,
    upper: ValueExpression) extends Predicate {

  override def name: String = s"${notOption.getOrElse("")}_${lower.name}_between_${upper.name}"

  override def sql: String = s"${notOption.getOrElse("")} BETWEEN ${lower.sql} AND ${upper.sql}"
}

object BetweenPredicate {
  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode]): BetweenPredicate = {

    val betweenPredicate = BetweenPredicate(querySession, parent, null, null, null)

    val notOption = if (RandomUtils.nextBoolean()) Some("NOT") else None

    val lower = ValueExpression(querySession, Some(betweenPredicate))
    val upper = ValueExpression(querySession, Some(betweenPredicate))

    betweenPredicate.copy(notOption = notOption, lower = lower, upper = upper)
  }
}
