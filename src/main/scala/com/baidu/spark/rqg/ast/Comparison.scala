package com.baidu.spark.rqg.ast
import com.baidu.spark.rqg.{BooleanType, DataType, RandomUtils}

import org.apache.spark.internal.Logging

case class Comparison(
    querySession: QuerySession,
    parent: Option[TreeNode],
    left: ValueExpression,
    right: ValueExpression,
    comparisonOperator: ComparisonOperator) extends ValueExpression {

  override def dataType: DataType[_] = BooleanType

  override def name: String = s"${left.name}_${comparisonOperator.name}_${right.name}"

  override def sql: String = s"(${left.sql}) ${comparisonOperator.operator} (${right.sql})"
}

object Comparison extends Logging {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): Comparison = {

    val comparison = Comparison(querySession.copy(), parent, null, null, null)

    val operators = Array(
      ComparisonOperator("EQ", "=="),
      ComparisonOperator("NEQ", "<>"),
      ComparisonOperator("NEQJ", "!="),
      ComparisonOperator("LT", "<"),
      ComparisonOperator("LTE", "<="),
      ComparisonOperator("GT", ">"),
      ComparisonOperator("GTE", ">="),
      ComparisonOperator("NSEQ", "<=>"))

    val comparisonOperator = RandomUtils.choice(operators)

    val (dataType, leftAvailableRelations, rightAvailableRelations) = parent match {
      // Rule #1: for join, left and right expression should come from left and right relations
      case Some(_: JoinCriteria) =>
        val leftDataTypes = querySession.availableRelations.flatMap(_.columns).map(_.dataType)
        val rightDataTypes = querySession.joiningRelations.flatMap(_.columns).map(_.dataType)
        val commonDataTypes = leftDataTypes.intersect(rightDataTypes).distinct
        val dataType = RandomUtils.choice(commonDataTypes)
        (dataType, querySession.availableRelations, querySession.joiningRelations)
      case _ =>
        val dataType = RandomUtils.choice(
          querySession.availableRelations.flatMap(_.dataTypes).distinct)
        (dataType, querySession.availableRelations, querySession.availableRelations)
    }

    logInfo(s"Generating Comparison with $dataType")

    val left =
      ValueExpression(
        querySession.copy(
          allowedDataTypes = Array(dataType), availableRelations = leftAvailableRelations),
        Some(comparison))

    val right =
      ValueExpression(
        querySession.copy(
          allowedDataTypes = Array(dataType), availableRelations = rightAvailableRelations),
        Some(comparison))

    comparison.copy(left = left, right = right, comparisonOperator = comparisonOperator)
  }
}

case class ComparisonOperator(name: String, operator: String)
