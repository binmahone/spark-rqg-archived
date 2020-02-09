package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.expressions.NamedExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * selectClause
 *     : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
 *     ;
 *
 * For now we don't support hint
 */
class SelectClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val setQuantifier: Option[String] = if (RandomUtils.nextBoolean()) Some("DISTINCT") else None
  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  private def generateNamedExpressionSeq: Seq[NamedExpression] = {
    (0 until RandomUtils.choice(1, 5))
      .map(_ => NamedExpression(querySession, Some(this)))
  }

  override def sql: String = s"SELECT " +
    s"${setQuantifier.getOrElse("")} " +
    s"${namedExpressionSeq.map(_.sql).mkString(", ")}"
}

/**
 * SelectClause generator
 */
object SelectClause extends TreeNodeGenerator[SelectClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): SelectClause = {
    new SelectClause(querySession, parent)
  }
}
