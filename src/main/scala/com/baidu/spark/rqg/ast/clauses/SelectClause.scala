package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.{RQGConfig, RandomUtils}
import com.baidu.spark.rqg.ast.expressions.NamedExpression
import com.baidu.spark.rqg.ast.{AggPreference, QuerySession, TreeNode, TreeNodeGenerator}

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

  val setQuantifier: Option[String] =
    if (RandomUtils.nextBoolean(querySession.rqgConfig.getProbability(RQGConfig.SELECT_DISTINCT))) {
      Some("DISTINCT")
    } else {
      None
    }
  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  private def generateNamedExpressionSeq: Seq[NamedExpression] = {
    val (min, max) = querySession.rqgConfig.getBound(RQGConfig.SELECT_ITEM_COUNT)
    (0 until RandomUtils.choice(min, max))
      .map { _ =>
        val useAgg = RandomUtils.nextBoolean()
        if (useAgg) {
          querySession.aggPreference = AggPreference.PREFER
        }
        val dataType = RandomUtils.choice(
          querySession.allowedDataTypes, querySession.rqgConfig.getWeight(RQGConfig.DATA_TYPE))
        val (min, max) = querySession.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
        querySession.allowedNestedExpressionCount = RandomUtils.choice(min, max)
        NamedExpression(querySession, Some(this), dataType, isLast = true)
      }
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
