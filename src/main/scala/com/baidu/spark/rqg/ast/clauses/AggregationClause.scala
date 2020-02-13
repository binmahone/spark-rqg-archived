package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{Query, QuerySession, TreeNode, TreeNodeGenerator}

/**
 * aggregationClause
 *     : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
 *       WITH kind=ROLLUP
 *     | WITH kind=CUBE
 *     | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
 *     | GROUP BY kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')'
 *     ;
 *
 * For now, we only support the first one.
 *
 * Usually, groupingExpressions is derived from selectClause, for example:
 * SELECT column_1 AS a, column_2 % 10 AS b, column_3 from table_1 GROUP BY a, b, column_3
 * So, the groupingExpressions maybe alias identifier(string actually) or real expression
 *
 * TODO: support expression after aggregation function is ready.
 */
class AggregationClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val groupingExpressions: Seq[String] = generateGroupingExpressions

  private def generateGroupingExpressions: Seq[String] = {
    val expressions = parent match {
      case Some(query: Query) =>
        query.selectClause.namedExpressionSeq.filterNot(_.isAgg).map(_.alias.get) ++
          query.selectClause.namedExpressionSeq.filter(_.isAgg).flatMap(_.nonAggColumns).map(_.sql)
      case _ =>
        throw new IllegalArgumentException("AggregationClause's parent is not Query")
    }
    if (expressions.isEmpty) {
      val relation = RandomUtils.nextChoice(querySession.availableRelations)
      val column = RandomUtils.nextChoice(relation.columns)
      Seq(s"${relation.name}.${column.name}")
    } else {
      expressions
    }
  }

  override def sql: String = s"GROUP BY ${groupingExpressions.mkString(", ")}"
}

/**
 * FromClause Generator
 */
object AggregationClause extends TreeNodeGenerator[AggregationClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): AggregationClause = {

    require(parent.forall(_.isInstanceOf[Query]), "AggregationClause can only be child of Query")
    new AggregationClause(querySession, parent)
  }
}