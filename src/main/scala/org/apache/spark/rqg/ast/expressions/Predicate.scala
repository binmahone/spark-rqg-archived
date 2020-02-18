package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.{DataType, RandomUtils}
import org.apache.spark.rqg.ast.{PredicateGenerator, QuerySession, TreeNode}

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
  val notOption: Option[String] = if (RandomUtils.nextBoolean()) {
    Some("NOT")
  } else {
    None
  }

  def name: String
}

/**
 * Predicate generator, random pick one class extends Predicate
 */
object Predicate extends PredicateGenerator[Predicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): Predicate = {

    val choice = RandomUtils.nextChoice(choices)
    choice.apply(querySession, parent, requiredDataType)
  }

  private def choices = {
    Array(BetweenPredicate, InPredicate, LikePredicate, NullPredicate)
  }
}

/**
 * grammar: NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
 */
class BetweenPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val lower: ValueExpression = generateLower
  val upper: ValueExpression = generateUpper

  private def generateLower = {
    ValueExpression(querySession, Some(this), requiredDataType)
  }

  private def generateUpper = {
    ValueExpression(querySession, Some(this), requiredDataType)
  }

  override def sql: String = s"${notOption.getOrElse("")} BETWEEN ${lower.sql} AND ${upper.sql}"

  override def name: String = "between_predicate"
}

/**
 * BetweenPredicate generator
 */
object BetweenPredicate extends PredicateGenerator[BetweenPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): BetweenPredicate = {
    new BetweenPredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: NOT? kind=IN '(' expression (',' expression)* ')'
 */
class InPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val expressionSeq: Seq[BooleanExpression] = generateExpressionSeq

  private def generateExpressionSeq = {
    Seq(BooleanExpression(querySession, Some(this), requiredDataType))
  }

  override def sql: String =
    s"${notOption.getOrElse("")} IN (${expressionSeq.map(_.sql).mkString(", ")})"

  override def name: String = "in_predicate"
}

/**
 * InPredicate generator
 */
object InPredicate extends PredicateGenerator[InPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): InPredicate = {
    new InPredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: NOT? kind=(RLIKE | LIKE) pattern=valueExpression
 */
class LikePredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {

  val valueExpression = ValueExpression(querySession, Some(this), requiredDataType)
  override def sql: String = s"${notOption.getOrElse("")} LIKE ${valueExpression.sql}"

  override def name: String = "like_predicate"
}

/**
 * LikePredicate generator
 */
object LikePredicate extends PredicateGenerator[LikePredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): LikePredicate = {
    new LikePredicate(querySession, parent, requiredDataType)
  }
}

/**
 * grammar: IS NOT? kind=NULL
 */
class NullPredicate(
    val querySession: QuerySession,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends Predicate {
  override def sql: String = s"IS ${notOption.getOrElse("")} NULL"

  override def name: String = "null_predicate"
}

/**
 * NullPredicate generator
 */
object NullPredicate extends PredicateGenerator[NullPredicate] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode],
      requiredDataType: DataType[_]): NullPredicate = {
    new NullPredicate(querySession, parent, requiredDataType)
  }
}

