package com.baidu.spark.rqg

import scala.util.Random

import com.baidu.spark.rqg.WindowBoundary._

class QueryProfile {
  // both low and high are inclusive
  protected val allBounds = Map(
    "MAX_NESTED_QUERY_COUNT" -> (0, 2),
    "MAX_NESTED_EXPR_COUNT" -> (0, 2),
    "SELECT_ITEM_COUNT" -> (1, 5),
    "WITH_TABLE_COUNT" -> (1, 3),
    "TABLE_COUNT" -> (1, 2),
    "ANALYTIC_LEAD_LAG_OFFSET" -> (1, 100),
    "ANALYTIC_WINDOW_OFFSET" -> (1, 100)
  )

  protected val allWeights = Map(
    "SELECT_ITEM_CATEGORY" -> Map(
      "AGG" -> 3,
      "ANALYTIC" -> 1,
      "BASIC" -> 10),
    "TYPES" -> Map(
      BooleanType.typeName -> 1,
      CharType.typeName -> 1,
      DecimalType.typeName -> 1,
      FloatType.typeName -> 1,
      IntType.typeName -> 10,
      TimestampType.typeName -> 0),
    "ANALYTIC_WINDOW" -> Map(
      s"ROWS, $UNBOUNDED_PRECEDING, None" -> 1,
      s"ROWS, $UNBOUNDED_PRECEDING, $PRECEDING" -> 2,
      s"ROWS, $UNBOUNDED_PRECEDING, $CURRENT_ROW" -> 1,
      s"ROWS, $UNBOUNDED_PRECEDING, $FOLLOWING" -> 2,
      s"ROWS, $UNBOUNDED_PRECEDING, $UNBOUNDED_FOLLOWING" -> 2,
      s"ROWS, $PRECEDING, None" -> 1,
      s"ROWS, $PRECEDING, $PRECEDING" -> 2,
      s"ROWS, $PRECEDING, $CURRENT_ROW" -> 1,
      s"ROWS, $PRECEDING, $FOLLOWING" -> 2,
      s"ROWS, $PRECEDING, $UNBOUNDED_FOLLOWING" -> 2,
      s"ROWS, $CURRENT_ROW, $None" -> 1,
      s"ROWS, $CURRENT_ROW, $CURRENT_ROW" -> 1,
      s"ROWS, $CURRENT_ROW, $FOLLOWING" -> 2,
      s"ROWS, $CURRENT_ROW, $UNBOUNDED_FOLLOWING" -> 2,
      s"ROWS, $FOLLOWING, $FOLLOWING" -> 2,
      s"ROWS, $FOLLOWING, $UNBOUNDED_FOLLOWING" -> 2,
      // Ranges not yet supported
      s"RANGE, $UNBOUNDED_PRECEDING, None" -> 0,
      s"RANGE, $UNBOUNDED_PRECEDING, $PRECEDING" -> 0,
      s"RANGE, $UNBOUNDED_PRECEDING, $CURRENT_ROW" -> 0,
      s"RANGE, $UNBOUNDED_PRECEDING, $FOLLOWING" -> 0,
      s"RANGE, $UNBOUNDED_PRECEDING, $UNBOUNDED_FOLLOWING" -> 0,
      s"RANGE, $PRECEDING, None" -> 0,
      s"RANGE, $PRECEDING, $PRECEDING" -> 0,
      s"RANGE, $PRECEDING, $CURRENT_ROW" -> 0,
      s"RANGE, $PRECEDING, $FOLLOWING" -> 0,
      s"RANGE, $PRECEDING, $UNBOUNDED_FOLLOWING" -> 0,
      s"RANGE, $CURRENT_ROW, None" -> 0,
      s"RANGE, $CURRENT_ROW, $CURRENT_ROW" -> 0,
      s"RANGE, $CURRENT_ROW, $FOLLOWING" -> 0,
      s"RANGE, $CURRENT_ROW, $UNBOUNDED_FOLLOWING" -> 0,
      s"RANGE, $FOLLOWING, $FOLLOWING" -> 0,
      s"RANGE, $FOLLOWING, $UNBOUNDED_FOLLOWING" -> 0),
    "JOIN" -> Map(
      "INNER" -> 90,
      "LEFT" -> 30,
      "RIGHT" -> 10,
      "FULL_OUTER" -> 3,
      "CROSS" -> 1),
    "SUBQUERY_PREDICATE" -> Map(
      "Exists, AGG, CORRELATED" -> 0,   // Not supported
      "Exists, AGG, UNCORRELATED" -> 0,
      "Exists, NON_AGG, CORRELATED" -> 0,
      "Exists, NON_AGG, UNCORRELATED" -> 0,
      "NotExists, AGG, CORRELATED" -> 0,   // Not supported
      "NotExists, AGG, UNCORRELATED" -> 0,   // Not supported
      "NotExists, NON_AGG, CORRELATED" -> 0,
      "NotExists, NON_AGG, UNCORRELATED" -> 0,   // Not supported
      "In, AGG, CORRELATED" -> 0,   // Not supported
      "In, AGG, UNCORRELATED" -> 0,   // Not supported
      "In, NON_AGG, CORRELATED" -> 0,
      "In, NON_AGG, UNCORRELATED" -> 0,
      "NotIn, AGG, CORRELATED" -> 0,   // Not supported
      "NotIn, AGG, UNCORRELATED" -> 0,
      "NotIn, NON_AGG, CORRELATED" -> 0,
      "NotIn, NON_AGG, UNCORRELATED" -> 0,
      "Scalar, AGG, CORRELATED" -> 0,   // Not supported
      "Scalar, AGG, UNCORRELATED" -> 0,
      "Scalar, NON_AGG, CORRELATED" -> 0,   // Not supported
      "Scalar, NON_AGG, UNCORRELATED" -> 0),
    "QUERY_EXECUTION" -> Map(   // Used by the discrepancy searcher
      "CREATE_TABLE_AS" -> 0,  // Not supported
      "RAW" -> 10,
      "VIEW" -> 0))  // Not supported

  protected val allProbabilities = Map(
    "OPTIONAL_QUERY_CLAUSES" -> Map(
      // Spark-specific change-> disable "WITH" for now because this seemed to be causing a
      // large number of false-positives because the query generator would produce invalid
      // subqueries that neither Spark nor Postgresql could parse.
      "WITH" -> 0.0d,   // MAX_NESTED_QUERY_COUNT bounds take precedence
      "FROM" -> 1.0d,
      "WHERE" -> 0.5d,
      "GROUP_BY" -> 0.1d,   // special case, doesn"t really do much, see comment above
      "HAVING" -> 0.25d,
      "UNION" -> 0.1d,
      "ORDER_BY" -> 0.1d),
    "OPTIONAL_ANALYTIC_CLAUSES" -> Map(
      "PARTITION_BY" -> 0.5d,
      "ORDER_BY" -> 0.5d,
      "WINDOW" -> 0.5d),   // will only be used if ORDER BY is chosen
    "MISC" -> Map(
      "INLINE_VIEW" -> 0.1d,   // MAX_NESTED_QUERY_COUNT bounds take precedence
      "SELECT_DISTINCT" -> 0.1d,
      "SCALAR_SUBQUERY" -> 0.0d,
      "UNION_ALL" -> 0.5d))

  protected val allFlags = Map(
    "ANALYTIC_DESIGNS" -> Map(
      "TOP_LEVEL_QUERY_WITHOUT_LIMIT" -> true,
      "DETERMINISTIC_ORDER_BY" -> true,
      "NO_ORDER_BY" -> true,
      "ONLY_SELECT_ITEM" -> true,
      "UNBOUNDED_WINDOW" -> true,
      "RANK_FUNC" -> true))

  private val random = new Random()

  private def choice[T](array: Array[T]): T = {
    val idx = random.nextInt(array.length)
    array(idx)
  }

  def bounds(key: String): (Int, Int) = {
    allBounds(key)
  }

  def weights(key: String): Map[String, Int] = {
    allWeights(key)
  }

  def probabilities(category: String, key: String): Double = {
    allProbabilities(category)(key)
  }

  def flags(key: String): Array[String] = {
    allFlags(key).toArray.filter(_._2).map(_._1)
  }

  private[rqg] def chooseFromWeights(weights: Map[String, Int]): String = {
    val totalWeights = weights.values.sum
    var predicate = random.nextDouble() * totalWeights

    // choose from weight in scala style
    // val cumWeights =
    //   weights.keys.zip(weights.values.toArray.scanLeft(0)(_ + _).sliding(2).toArray)
    // cumWeights.find(x => predicate >= x._2(0) && predicate < x._2(1)).map(_._1).getOrElse("")

    for ((choice, w) <- weights) {
      if (w > 0 && w > predicate) {
        return choice
      }
      predicate -= w
    }
    throw new Exception("never reach here")
  }

  private def chooseFromWeights(key: String): String = {
    chooseFromWeights(weights(key))
  }

  private def chooseFromFilteredWeights(
      p: String => Boolean, weights: Map[String, Int]): String = {
    chooseFromWeights(weights.filter(e => p(e._1)))
  }

  private def chooseFromFilteredWeights(
      p: String => Boolean, key: String): String = {
    chooseFromFilteredWeights(p, weights(key))
  }

  private def chooseFromBounds(key: String): Int = {
    chooseFromBounds(bounds(key)._1, bounds(key)._2)
  }

  private def chooseFromBounds(lo: Int, hi: Int): Int = {
    random.nextInt(hi - lo + 1) + lo
  }

  private def decideFromProbability(category: String, key: String): Boolean = {
    random.nextDouble() < probabilities(category, key)
  }

  def getMaxNestedQueryCount: Int = chooseFromBounds("MAX_NESTED_QUERY_COUNT")

  def useWithClause: Boolean = decideFromProbability("OPTIONAL_QUERY_CLAUSES", "WITH")

  def getWithClauseTableRefCount: Int = chooseFromBounds("WITH_TABLE_COUNT")

  def getSelectItemCount: Int = chooseFromBounds("SELECT_ITEM_COUNT")

  def chooseNestedExprCount: Int = chooseFromBounds("MAX_NESTED_EXPR_COUNT")

  def allowedAnalyticDesigns: Array[String] = flags("ANALYTIC_DESIGNS")

  def usePartitionByClauseInAnalytic: Boolean =
    decideFromProbability("OPTIONAL_ANALYTIC_CLAUSES", "PARTITION_BY")

  def useOrderByClauseInAnalytic: Boolean =
    decideFromProbability("OPTIONAL_ANALYTIC_CLAUSES", "ORDER_BY")

  def useWindowInAnalytic: Boolean =
    decideFromProbability("OPTIONAL_ANALYTIC_CLAUSES", "WINDOW")

  def chooseWindowType: String = chooseFromWeights("ANALYTIC_WINDOW")

  def getWindowOffset: Int = chooseFromBounds("ANALYTIC_WINDOW_OFFSET")

  def getOffsetForAnalyticLeadOrLag: Int = chooseFromBounds("ANALYTIC_LEAD_LAG_OFFSET")

  def getTableCount: Int = chooseFromBounds("TABLE_COUNT")

  def useInlineView: Boolean = decideFromProbability("MISC", "INLINE_VIEW")

  def chooseTable(tableExprs: Array[TableExpr]): TableExpr =
    tableExprs(random.nextInt(tableExprs.length))

  def chooseJoinType(joinTypes: Set[DataType]): String =
    chooseFromFilteredWeights((joinType: String) => joinType == "AAA", "JOIN")

  def getJoinConditionCount: Int = chooseFromBounds("MAX_NESTED_EXPR_COUNT")

  def useWhereClause: Boolean = decideFromProbability("OPTIONAL_QUERY_CLAUSES", "WHERE")

  def useScalarSubquery: Boolean = decideFromProbability("MISC", "SCALAR_SUBQUERY")

  def chooseSubqueryPredicateCategory(funcName: String, allowCorrelated: Boolean = true): String = {

    val subqueryWeights = weights("SUBQUERY_PREDICATE")
    val selectedFuncName = subqueryWeights.keys
      .map(x => x.split(",")(0))
      .find(_ == funcName).getOrElse("Scalar")

    val useAgg = weights("SELECT_ITEM_CATEGORY")("AGG") > 0
    val useCorrelated = allowCorrelated && bounds("TABLE_COUNT")._2 == 0

    val finalWeights = subqueryWeights.filter {
      case (key, value) =>
        val name = key.split(",")(0)
        val agg = key.split(",")(1)
        val correlated = key.split(",")(2)
        name == selectedFuncName &&
          (useAgg || agg == "NON_AGG") &&
          (useCorrelated || correlated == "UNCORRELATED")
    }
    chooseFromWeights(finalWeights)
  }

  def useDistinct: Boolean = decideFromProbability("MISC", "SELECT_DISTINCT")

  def useDistinctInFunc: Boolean = decideFromProbability("MISC", "SELECT_DISTINCT")

  def useGroupByClause: Boolean = decideFromProbability("OPTIONAL_QUERY_CLAUSES", "GROUP_BY")

  def useHavingClause: Boolean = decideFromProbability("OPTIONAL_QUERY_CLAUSES", "HAVING")

  def useUnionClause: Boolean = decideFromProbability("OPTIONAL_QUERY_CLAUSES", "UNION")

  def useUnionAll: Boolean = decideFromProbability("MISC", "UNION_ALL")

  def getQueryExecution: String = chooseFromWeights("QUERY_EXECUTION")

  def useHavingWithoutGroupby: Boolean = true

  def useNestedWith: Boolean = true

  // Workaround for Hive null ordering differences, and lack of "NULL FIRST", "NULL LAST"
  // specifications. The ref db will order nulls as specified for ASC sorting to make it
  // identical to Hive. Valid return values are: "BEFORE", "AFTER", or "DEFAULT",
  // the latter means no specification needed.
  def nullsOrderAsc: String = "DEFAULT"

  def chooseValExpr(valExprs: Array[ValExpr], types: Set[DataType] = DataType.TYPES): ValExpr = {
    require(valExprs.nonEmpty, "At least on value is required")
    require(types.nonEmpty, "At least one type is required")

    val groupedColumns = RQGUtils.groupValExprByColType(valExprs)
    val availableTypes = types & groupedColumns.keys.toSet
    if (availableTypes.isEmpty) {
      throw new IllegalArgumentException(
        "None of the provided values return any of the required types")
    }

    choice(groupedColumns(chooseType(availableTypes)))
  }

  def chooseConstant(allowNull: Boolean = true): Literal = {
    chooseConstant(chooseType(), allowNull)
  }

  def chooseConstant(requiredType: DataType, allowNull: Boolean = true): Literal = {
    // TODO: random val generator
    Literal(1, IntType)
  }

  def chooseType(types: Set[DataType] = DataType.TYPES): DataType = {
   val typeWeights = weights("TYPES")
   DataType.nameToType(chooseFromWeights(typeWeights.filter(x => types.exists(_.typeName == x._1))))
  }
}
