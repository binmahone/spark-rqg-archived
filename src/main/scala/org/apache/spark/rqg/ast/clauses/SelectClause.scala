package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{ArrayType, DataType, MapType, RQGConfig, RandomUtils, StructType}
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
    val parent: Option[TreeNode],
    val requiredReturnType: Option[DataType[_]]) extends TreeNode {

  val namedExpressionSeq: Seq[NamedExpression] = generateNamedExpressionSeq

  val setQuantifier: Option[String] = {
    // In Spark, set operations such as DISTINCT do not support the map type, so don't apply it if
    // any of the expressions do not support maps.
    val hasMaps = namedExpressionSeq.exists(e => DataType.containsMap(e.dataType))
    val useDistinct = RandomUtils.nextBoolean(
      queryContext.rqgConfig.getProbability(RQGConfig.SELECT_DISTINCT))
    Some("DISTINCT").filter(_ => useDistinct && !hasMaps)
  }

  private def generateNamedExpressionSeq: Seq[NamedExpression] = {
    if (parent.get.isInstanceOf[NestedQuery]) {
      // TODO: Only if it is a scalar subquery we can only generate at most one column, this can
      //  be done after the function framework is done as scalar subquery should be generated
      //  inside a aggregating function
      require(requiredReturnType.nonEmpty)
      requiredReturnType.map(generate).toSeq
    } else {
      val (minExprs, maxExprs) = queryContext.rqgConfig.getBound(RQGConfig.SELECT_ITEM_COUNT)
      val numExpressions = RandomUtils.choice(minExprs, maxExprs)
      (0 until numExpressions).map { _ =>
        // `allDataTypes` okay since the weights will be used to filter out unsupported types.
        val dataType = RandomUtils.generateRandomDataType(DataType.allDataTypes,
          weightsOpt = Some(queryContext.rqgConfig.getWeight(RQGConfig.QUERY_DATA_TYPE)))
        generate(dataType)
      }
    }
  }

  /**
   * Generate a single expression with the given `dataType`.
   */
  private def generate(dataType: DataType[_]): NamedExpression = {
    // PHOTON: Don't use aggregations with arrays and structs, since Photon does not support them.
    val useAgg = RandomUtils.nextBoolean() && !dataType.isInstanceOf[ArrayType] && !dataType.isInstanceOf[StructType]
    if (useAgg) {
      queryContext.aggPreference = AggPreference.PREFER
    }
    val (minNested, maxNested) = queryContext.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
    queryContext.allowedNestedExpressionCount = RandomUtils.choice(minNested, maxNested)
    NamedExpression(queryContext, Some(this), dataType, isLast = true)
  }

  override def sql: String = s"SELECT " +
    s"${setQuantifier.getOrElse("")} " +
    s"${namedExpressionSeq.map(_.sql).mkString(", ")}"
}

/**
 * SelectClause generator
 */
object SelectClause extends TreeNodeWithParent[SelectClause] {
  def apply(querySession: QueryContext, parent: Option[TreeNode],
      requiredReturnType: Option[DataType[_]] = None): SelectClause = {
    new SelectClause(querySession, parent, requiredReturnType)
  }
}
