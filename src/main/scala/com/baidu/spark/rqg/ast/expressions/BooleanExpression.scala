package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.clauses.SelectClause
import com.baidu.spark.rqg.ast.relations.JoinCriteria
import com.baidu.spark.rqg.{DataType, RandomUtils}

import org.apache.spark.internal.Logging

trait BooleanExpression extends TreeNode {

  def dataType: DataType[_]

  def name: String
}

object BooleanExpression extends Logging {

  private val maxNestedExpressionCount = 1

  def apply(
    querySession: QuerySession,
    parent: Option[TreeNode]): BooleanExpression = {

    val choices = Array("Predicated", "LogicalNot")

    val filteredChoices = parent match {
      case Some(_: JoinCriteria) =>
        // Rule #1: Only Generate Predicated for JoinCriteria
        choices.filter(_ == "Predicated")
      case Some(NamedExpression(_, Some(_: SelectClause), _, _)) =>
        // Rule #2: SelectClause should filter some choices
        logInfo("Generating Predicated for SelectClause")
        choices.filter(_ == "Predicated")
      case _ =>
        choices
    }

    // Rule #3: Only generate Predicated when reach max nested count
    val filteredChoices2 = if (querySession.nestedExpressionCount > maxNestedExpressionCount) {
      logInfo(s"Generating BooleanExpression reach max nested count: $maxNestedExpressionCount")
      filteredChoices.filter(_ == "Predicated")
    } else {
      filteredChoices
    }
    querySession.nestedExpressionCount += 1

    RandomUtils.choice(filteredChoices2) match {
      case "Predicated" => Predicated(querySession, parent)
      case "LogicalNot" => LogicalNot(querySession, parent)
    }
  }
}
