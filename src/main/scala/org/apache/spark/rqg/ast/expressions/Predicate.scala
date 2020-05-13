package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.ast.clauses.WhereClause
import org.apache.spark.rqg.{DataType, RQGConfig, RandomUtils}
import org.apache.spark.rqg.ast.{NestedQuery, PredicateGenerator, QueryContext, TreeNode}

/**
 * predicate
 *     : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
 *     | NOT? kind=IN '(' expression (',' expression)* ')'
 *     | NOT? kind=IN '(' query ')'
 *     | NOT? kind=(RLIKE | LIKE) pattern=valueExpression
 *     | IS NOT? kind=NULL
 *     | IS NOT? kind=(TRUE | FALSE | UNKNOWN)
 *     | IS NOT? kind=DISTINCT FROM right=valueExpression
 *     ;
 *
 * Predicate is not an Expression. it actually is part of an expression but contains expressions
 * as its children
 *
 * For now, we only support BETWEEN, IN, LIKE, NULL predicate
 */
trait Predicate extends TreeNode {
  def name: String

  def isAgg: Boolean
}

/**
 * Predicate generator, random pick one class extends Predicate
 */
object Predicate extends PredicateGenerator[Predicate] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): Predicate = {
    val choice = RandomUtils.nextChoice(choices)
    val predicate = choice.apply(querySession, parent, requiredDataType)
    predicate
  }
  private def choices = {
    Array(BetweenPredicate, InPredicate, LikePredicate, NullPredicate)
  }
}

/**
 * grammar: NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
 */
class BetweenPredicate(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val lower: ValueExpression = generateLower
  val upper: ValueExpression = generateUpper

  private def generateLower = {
    ValueExpression(queryContext, Some(this), requiredDataType)
  }

  private def generateUpper = {
    ValueExpression(queryContext, Some(this), requiredDataType)
  }

  override def sql: String = s"${RandomUtils.getNotOrEmpty()} BETWEEN ${lower.sql} AND ${upper.sql}"

  override def name: String = "between_predicate"

  override def isAgg: Boolean = lower.isAgg || upper.isAgg
}

/**
 * BetweenPredicate generator
 */
object BetweenPredicate extends PredicateGenerator[BetweenPredicate] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): BetweenPredicate = {
    new BetweenPredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: NOT? kind=IN '(' expression (',' expression)* ')'
 */
class InPredicate(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  var subQuery: Option[NestedQuery] = generateSubQuery()
  val expressionSeq: Seq[BooleanExpression] = generateExpressionSeq

    private def generateSubQuery(): Option[NestedQuery] = {
      if (parent.get.parent.get.isInstanceOf[WhereClause] && RandomUtils.nextBoolean(queryContext.rqgConfig.getProbability(RQGConfig.NESTED_IN))) {
        val ans = Some(NestedQuery(
          QueryContext(availableTables = queryContext.availableTables,
            rqgConfig = queryContext.rqgConfig,
            allowedNestedSubQueryCount = queryContext.allowedNestedSubQueryCount,
            nextAliasId = queryContext.nextAliasId + 1),
          Some(this), Some(requiredDataType)))
        return ans
      }
      None
    }

  private def generateExpressionSeq = {
    Seq(BooleanExpression(queryContext, Some(this), requiredDataType))
  }

  override def sql: String =
    if (subQuery.isDefined) {
      s"${RandomUtils.getNotOrEmpty()} IN (${subQuery.get.sql})"
    } else {
      s"${RandomUtils.getNotOrEmpty()} IN (${expressionSeq.map(_.sql).mkString(", ")})"
    }

  override def name: String = "in_predicate"

  override def isAgg: Boolean = {
    for (x <- expressionSeq) {
      if (x.isAgg) {
        return true
      }
    }
    false
  }
}

/**
 * InPredicate generator
 */
object InPredicate extends PredicateGenerator[InPredicate] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): InPredicate = {
    new InPredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: NOT? kind=(RLIKE | LIKE) pattern=valueExpression
 */
class LikePredicate(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val valueExpression = ValueExpression(queryContext, Some(this), requiredDataType)
  override def sql: String = s"${RandomUtils.getNotOrEmpty()} LIKE ${valueExpression.sql}"

  override def name: String = "like_predicate"

  override def isAgg: Boolean = valueExpression.isAgg
}

/**
 * LikePredicate generator
 */
object LikePredicate extends PredicateGenerator[LikePredicate] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): LikePredicate = {
    new LikePredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: IS NOT? kind=NULL
 */
class NullPredicate(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {
  override def sql: String = s"IS ${RandomUtils.getNotOrEmpty()} NULL"

  override def name: String = "null_predicate"

  override def isAgg: Boolean = false
}

/**
 * NullPredicate generator
 */
object NullPredicate extends PredicateGenerator[NullPredicate] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): NullPredicate = {
    new NullPredicate(querySession, parent, requiredDataType)
  }
}

