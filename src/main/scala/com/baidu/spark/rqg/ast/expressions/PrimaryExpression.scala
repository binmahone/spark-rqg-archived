package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.clauses.SelectClause
import com.baidu.spark.rqg.ast.relations.JoinCriteria
import com.baidu.spark.rqg.{DataType, RandomUtils}

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
      case _ =>
        choices
    }

    RandomUtils.choice(filteredChoices) match {
      case "ColumnReference" => ColumnReference(querySession, parent)
      case "ConstantDefault" => ConstantDefault(querySession, parent)
    }
  }
}
