package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{DataType, MapType, RQGConfig, RandomUtils, StructType}
import org.apache.spark.rqg.ast.expressions.NamedExpression
import org.apache.spark.rqg.ast.{AggPreference, NestedQuery, QueryContext, TreeNode, TreeNodeWithParent}

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

  // Important: We have to generate namedExpressionSeq before setQuantifier
  // because setQuantifier depends on namedExpressionSeq dataType
  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  // DISTINCT behind the scene use the set operation but set does not support maptype and structtype
  // Therefore, we need to exclude two of them here
  val setQuantifier: Option[String] =
    if (!namedExpressionSeq.head.dataType.isInstanceOf[MapType] &&
        !namedExpressionSeq.head.dataType.isInstanceOf[StructType] &&
        RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.SELECT_DISTINCT))) {
      Some("DISTINCT")
    } else {
      None
    }

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
    val randomChoiceDataType = RandomUtils.choice(
      queryContext.allowedDataTypes, queryContext.rqgConfig.getWeight(RQGConfig.QUERY_DATA_TYPE))
    val dataType = requiredDataType.getOrElse(randomChoiceDataType)
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
