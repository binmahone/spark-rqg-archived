package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.{BooleanType, DataType, RQGConfig, RandomUtils}
import com.baidu.spark.rqg.ast.expressions.BooleanExpression
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * joinCriteria
 *     : ON booleanExpression
 *     | USING identifierList
 *
 * according sqlbase.g4, the join condition generation is a little bit complex:
 * booleanExpression -> predicated -> valueExpression -> comparison ->
 *   left: valueExpression -> primaryExpression -> columnReference
 *   right: valueExpression -> primaryExpression -> columnReference
 *
 * what's more, we need add some constrains to make sure the query can produce data:
 *   1. only data types support join can be used during booleanExpression generation
 *   2. left and right in comparison should choose from left and right tables in joining
 *   3. make sure booleanExpression always generate comparison(or other relational func) for join
 */
class JoinCriteria(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  require(querySession.joiningRelation.isDefined, "no relation to join during creating JoinCriteria")

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression: BooleanExpression = {
    val prevAllowedDataTypes = querySession.allowedDataTypes
    querySession.requiredRelationalExpressionCount = 1
    val (min, max) = querySession.rqgConfig.getBound(RQGConfig.MAX_NESTED_EXPR_COUNT)
    // we always need at least one nested for join criteria
    querySession.allowedNestedExpressionCount = RandomUtils.choice(math.max(min, 1), max)
    val booleanExpression = BooleanExpression(querySession, Some(this), BooleanType, isLast = true)
    querySession.allowedDataTypes = prevAllowedDataTypes
    assert(querySession.requiredRelationalExpressionCount <= 0)
    // restore back
    querySession.requiredRelationalExpressionCount = 0
    booleanExpression
  }

  override def sql: String = s"ON ${booleanExpression.sql}"
}

/**
 * JoinCriteria Generator
 */
object JoinCriteria extends TreeNodeGenerator[JoinCriteria] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): JoinCriteria = {
    new JoinCriteria(querySession, parent)
  }
}
