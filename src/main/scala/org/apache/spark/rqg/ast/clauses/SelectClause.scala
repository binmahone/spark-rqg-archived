package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{DataType, RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.expressions.NamedExpression
import org.apache.spark.rqg.ast.{AggPreference, NestedQuery, QueryContext, TreeNodeWithParent, TreeNode}

/**
 * selectClause
 *     : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
 *     ;
 *
 * For now we don't support hint
 */
class SelectClause(
    val queryContext: QueryContext,
    val requiredDataType: Option[DataType[_]],
    val parent: Option[TreeNode]) extends TreeNode {

  val setQuantifier: Option[String] =
    if (RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.SELECT_DISTINCT))) {
      Some("DISTINCT")
    } else {
      None
    }
  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  private def generate(minSelectCount: Int, maxSelectCount: Int, dataType: DataType[_]): Seq[NamedExpression] = {
    (0 until RandomUtils.choice(minSelectCount, maxSelectCount))
      .map { _ => {
        val useAgg = RandomUtils.nextBoolean()
        if (useAgg) {
          queryContext.aggPreference = AggPreference.PREFER
        }
        val (minNested, maxNested) = queryContext.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
        queryContext.allowedNestedExpressionCount = RandomUtils.choice(minNested, maxNested)
        NamedExpression(queryContext, Some(this), dataType, isLast = true)
      }}
  }

  private def generateNamedExpressionSeq: Seq[NamedExpression] = {
    var (min, max) = queryContext.rqgConfig.getBound(RQGConfig.SELECT_ITEM_COUNT)
    val dataType = requiredDataType.getOrElse(RandomUtils.choice(
      queryContext.allowedDataTypes, queryContext.rqgConfig.getWeight(RQGConfig.QUERY_DATA_TYPE)))
    // TODO: Only if it is a scalar subquery we can only generate at most one column, this can
    //  be done after the function framework is done as scalar subquery should be generated
    //  inside a aggregating function
    if (parent.get.isInstanceOf[NestedQuery]) {
      min = 1
      max = 1
    }
    generate(min, max, dataType)
  }

  override def sql: String = s"SELECT " +
    s"${setQuantifier.getOrElse("")} " +
    s"${namedExpressionSeq.map(_.sql).mkString(", ")}"
}

/**
 * SelectClause generator
 */
object SelectClause extends TreeNodeWithParent[SelectClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: Option[DataType[_]] = None): SelectClause = {
    new SelectClause(querySession, requiredDataType, parent)
  }
}
