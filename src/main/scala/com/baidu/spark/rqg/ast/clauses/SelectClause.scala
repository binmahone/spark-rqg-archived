package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.expressions.NamedExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

import org.apache.spark.internal.Logging

case class SelectClause(
    querySession: QuerySession,
    parent: Option[TreeNode],
    setQuantifier: Option[String],
    namedExpressionSeq: Array[NamedExpression]) extends TreeNode {

  require(namedExpressionSeq == null || namedExpressionSeq.length > 0)

  override def sql: String =
    s"SELECT ${setQuantifier.getOrElse("")} ${namedExpressionSeq.map(_.sql).mkString(", ")}"
}

object SelectClause extends Logging {
  def apply(querySession: QuerySession, parent: Option[TreeNode]): SelectClause = {

    val selectClause = SelectClause(querySession, parent, null, null)

    val setQuantifier = if (RandomUtils.nextBoolean()) Some("DISTINCT") else None

    val namedExpressionSeq =
      (0 until RandomUtils.choice(1, 5))
        .map(_ => NamedExpression(querySession.copy(), Some(selectClause))).toArray

    logInfo(s"Generating SelectClause with ${namedExpressionSeq.length} items")

    selectClause.copy(setQuantifier = setQuantifier, namedExpressionSeq = namedExpressionSeq)
  }
}
