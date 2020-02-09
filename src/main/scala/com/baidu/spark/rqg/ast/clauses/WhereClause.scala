package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.BooleanType
import com.baidu.spark.rqg.ast.expressions.BooleanExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * whereClause
 *     : WHERE booleanExpression
 *     ;
 */
class WhereClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    BooleanExpression(querySession, Some(this), BooleanType, isLast = true)
  }
  override def sql: String = s"WHERE ${booleanExpression.sql}"
}

/**
 * WhereClause generator
 */
object WhereClause extends TreeNodeGenerator[WhereClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): WhereClause = {

    new WhereClause(querySession, parent)
  }
}
