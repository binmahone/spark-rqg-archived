package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.expressions.NamedExpression
import org.apache.spark.rqg.ast.{AggPreference, QueryContext, TreeNode, TreeNodeGenerator}

/**
 * selectClause
 *     : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
 *     ;
 *
 * For now we don't support hint
 */
class SelectClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val setQuantifier: Option[String] =
    if (RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.SELECT_DISTINCT))) {
      Some("DISTINCT")
    } else {
      None
    }
  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  private def generateNamedExpressionSeq: Seq[NamedExpression] = {
    val (min, max) = queryContext.rqgConfig.getBound(RQGConfig.SELECT_ITEM_COUNT)
    (0 until RandomUtils.choice(min, max))
      .map { _ =>
        val useAgg = RandomUtils.nextBoolean()
        if (useAgg) {
          queryContext.aggPreference = AggPreference.PREFER
        }
        val dataType = RandomUtils.choice(
          queryContext.allowedDataTypes, queryContext.rqgConfig.getWeight(RQGConfig.DATA_TYPE))
        val (min, max) = queryContext.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
        queryContext.allowedNestedExpressionCount = RandomUtils.choice(min, max)
        NamedExpression(queryContext, Some(this), dataType, isLast = true)
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
      querySession: QueryContext,
      parent: Option[TreeNode]): SelectClause = {
    new SelectClause(querySession, parent)
  }
}
