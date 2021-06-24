package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.{BooleanType, RQGEmptyChoiceException, RandomUtils}
import org.apache.spark.rqg.ast.expressions.BooleanExpression
import org.apache.spark.rqg.ast.relations.RelationPrimary
import org.apache.spark.rqg.ast.{AggPreference, NestedQuery, Query, QueryContext, TreeNode, TreeNodeGenerator}

/**
 * havingClause
 *  : HAVING booleanExpression
 *  ;
 */
class HavingClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  // select a relation
  private val relation: RelationPrimary = generateRelation

  private def generateRelation = {
    if (queryContext.needColumnFromJoiningRelation) {
      queryContext.joiningRelation.getOrElse {
        throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
      }
    } else {
      RandomUtils.nextChoice(
        queryContext.availableRelations)
    }
  }

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    // group-by selects all non-aggregated columns from select clause
    // so we exclude these columns in the availableColumns
    val nonAggColumns = parent match {
      case Some(query: Query) =>
        query.selectClause.namedExpressionSeq.flatMap(_.nonAggColumns)
      case Some(nestedQuery: NestedQuery) =>
        nestedQuery.selectClause.namedExpressionSeq.flatMap(_.nonAggColumns)
      case _ =>
        throw new IllegalArgumentException("AggregationClause's parent is not Query")
    }

    val availableColumns = relation.flattenedColumns.filter(c => nonAggColumns.forall(na => na.column != c))
    if (availableColumns.isEmpty) {
      throw RQGEmptyChoiceException("HavingClause no available column references")
    }

    // we set aggPreference = AggPreference.PREFER && allowedNestedExpressionCount = 1
    // to generate an agg function
    queryContext.aggPreference = AggPreference.PREFER
    queryContext.allowedNestedExpressionCount = 1
    queryContext.availableColumns = Some(availableColumns)
    val ret = BooleanExpression(queryContext, Some(this), BooleanType, isLast = true)
    queryContext.availableColumns = None
    ret
  }
  override def sql: String = s"HAVING ${booleanExpression.sql}"
}

object HavingClause extends TreeNodeGenerator[HavingClause] {
  def apply(querySession: QueryContext, parent: Option[TreeNode]): HavingClause = {
    require(parent.forall(x => x.isInstanceOf[Query] || x.isInstanceOf[NestedQuery]), "HavingClause can only be child of Query")
    new HavingClause(querySession, parent)
  }
}
