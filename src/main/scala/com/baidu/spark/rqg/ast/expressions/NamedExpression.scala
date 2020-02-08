package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

/**
 * namedExpression
 *     : expression (AS? (name=errorCapturingIdentifier | identifierList))?
 *     ;
 */
class NamedExpression(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val expression: BooleanExpression = generateExpression

  val alias = Some(querySession.nextAlias(expression.name))

  private def generateExpression: BooleanExpression = {
    val dataType = RandomUtils.choice(querySession.allowedDataTypes)
    BooleanExpression(querySession.copy(), parent, dataType)
  }

  def name: String = alias.getOrElse(expression.name)

  def sql: String = s"${expression.sql} ${alias.map("AS " + _).getOrElse("")}"
}

object NamedExpression {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): NamedExpression = {
    new NamedExpression(querySession, parent)
  }
}

