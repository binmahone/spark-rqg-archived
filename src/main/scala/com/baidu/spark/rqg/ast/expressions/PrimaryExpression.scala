package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.clauses.{AggregationClause, SelectClause}
import com.baidu.spark.rqg.ast.relations.JoinCriteria
import com.baidu.spark.rqg.{DataType, RandomUtils, Utils}

import org.apache.spark.internal.Logging

trait PrimaryExpression extends ValueExpression {

  def dataType: DataType[_]

  def name: String
}

object PrimaryExpression extends Logging {

  def apply(querySession: QuerySession, parent: Option[TreeNode]): PrimaryExpression = {

    val choices = Array("ColumnReference", "ConstantDefault")

    val filteredChoices = parent match {
      // Rule #1: SelectClause should filter some choices
      case Some(Predicated(_, Some(NamedExpression(_, Some(_: SelectClause), _, _)), _, _)) =>
        logInfo("Generating PrimaryExpression for SelectClause")
        choices.filter(_ == "ColumnReference")
      case Some(Comparison(_, Some(Predicated(_, Some(_: JoinCriteria), _, _)), _, _, _)) =>
        logInfo("Generating PrimaryExpression for Comparision of JoinCriteria")
        choices.filter(_ == "ColumnReference")
      case Some(Predicated(_, Some(_: AggregationClause), _, _)) =>
        logInfo("Generating PrimaryExpression for AggregationClause")
        choices.filter(_ == "ColumnReference")
      case _ =>
        choices
    }

    // Rule #3: if allowed relations has no allowed data type, don't generate ColumnReference
    val filteredChoices2 = if (Utils.allowedRelations(
      querySession.availableRelations, querySession.allowedDataTypes).isEmpty) {
      filteredChoices.filterNot(_ == "ColumnReference")
    } else {
      filteredChoices
    }

    RandomUtils.choice(filteredChoices2) match {
      case "ColumnReference" => ColumnReference(querySession, parent)
      case "ConstantDefault" => ConstantDefault(querySession, parent)
    }
  }
}
