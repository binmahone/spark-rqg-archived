package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.Utils

case class NamedExpression(
    querySession: QuerySession,
    parent: Option[TreeNode],
    expression: ValueExpression,
    alias: Option[String]) extends TreeNode {

  def name: String = alias.getOrElse(expression.name)

  def sql: String = s"${expression.sql} ${alias.map("AS " + _).getOrElse("")}"
}

object NamedExpression {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): NamedExpression = {

    val namedExpression = NamedExpression(querySession, parent, null, null)

    val expression = generateExpression(querySession, Some(namedExpression))

    val alias = Some(Utils.nextAlias(expression.name))

    namedExpression.copy(expression = expression, alias = alias)
  }

  private def generateExpression(
      querySession: QuerySession,
      parent: Option[TreeNode]): ValueExpression = {

    ValueExpression(querySession.copy(), parent)
  }
}
