package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{BooleanType, DataType, RandomUtils}

import org.apache.spark.internal.Logging

trait ValueExpression extends TreeNode {

  def dataType: DataType[_]

  def name: String

  def sql: String = name
}

object ValueExpression extends Logging {

  private val maxNestedExpressionCount = 1

  def apply(querySession: QuerySession, parent: Option[TreeNode]): ValueExpression = {

    val choices = Array("PrimaryExpression", "Comparison")

    val filteredChoices = parent match {
      case Some(NamedExpression(_, Some(_: SelectClause), _, _)) =>
        // Rule #1: SelectClause should filter some choices
        logInfo("Generating ValueExpression for SelectClause")
        choices.filter(_ == "PrimaryExpression")
      case Some(_: JoinCriteria) =>
        // Rule #2: Only Generate Comparison for JoinCriteria
        logInfo("Generating ValueExpression for JoinCriteria")
        choices.filter(_ == "Comparison")
      case Some(Comparison(_, Some(_: JoinCriteria), _, _, _)) =>
        // Rule #3: Only Generate PrimaryExpression for Comparison of JoinCriteria
        logInfo("Generating ValueExpression for Comparison of JoinCriteria")
        choices.filter(_ == "PrimaryExpression")
      case _ =>
        choices
    }

    // Rule #3: Only generate PrimaryExpression when reach max nested count
    val filteredChoices2 = if (querySession.nestedExpressionCount > maxNestedExpressionCount) {
      logInfo(s"Generating ValueExpression reach max nested count: $maxNestedExpressionCount")
      filteredChoices.filter(_ == "PrimaryExpression")
    } else {
      filteredChoices
    }
    querySession.nestedExpressionCount += 1

    // Rule #4: Filter out Comparison if BooleanType is not allowed
    val filteredChoices3 = if (!querySession.allowedDataTypes.contains(BooleanType)) {
      logInfo("Generating ValueExpression while BooleanType is not allowed")
      filteredChoices.filterNot(_ == "Comparison")
    } else {
      filteredChoices2
    }

    RandomUtils.choice(filteredChoices3) match {
      case "PrimaryExpression" => PrimaryExpression(querySession, parent)
      case "Comparison" => Comparison(querySession, parent)
    }
  }
}
