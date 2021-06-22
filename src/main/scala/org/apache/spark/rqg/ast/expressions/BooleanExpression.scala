package org.apache.spark.rqg.ast.expressions

import org.apache.spark.rqg.{BooleanType, DataType, RandomUtils}
import org.apache.spark.rqg.ast._
import org.apache.spark.rqg.ast.clauses.SelectClause
import org.apache.spark.rqg.ast.operators._

/**
 * booleanExpression
 *   : NOT booleanExpression                                        #logicalNot
 *   | EXISTS '(' query ')'                                         #exists
 *   | valueExpression predicate?                                   #predicated
 *   | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
 *   | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
 *   ;
 *
 * BooleanExpression is the root class of all expressions defined in sqlbase.g4.
 * Generate a booleanExpression will generate a nested expression randomly. Here is an example:
 *
 * A >> B means: create A will randomly choose to create one of its sub-class (B)
 * A -> B means: create A will create B as its child
 *
 *                                                          /-> valueExpression >> PrimaryExpression >> Column
 * booleanExpression       /-> booleanExpression >> predicated
 *        |>> logicalBinary -> AND                         \-> "IS NOT NULL"
 *                         \-> booleanExpression                                     /-> valueExpression >> primaryExpression >> column
 *                                    |>> predicated -> valueExpression >> comparison -> EQ
 *                                                                                   \-> valueExpression >> primaryExpression >> constant
 *
 * (column_1 IS NOT NULL) AND (column_b eq 0)
 * Here the name may lead some ambiguous: BooleanExpression will also generate non-boolean
 * expression. Since booleanExpression may generate a int constant (see above example)
 */
trait BooleanExpression extends TreeNode with Expression

/**
 * BooleanExpression Generator. It randomly generate one class extends BooleanExpression
 */
object BooleanExpression extends ExpressionGenerator[BooleanExpression] {
  override def apply(
      queryContext: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): BooleanExpression = {

    val filteredChoices = if (queryContext.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else if (queryContext.needGenerateRelationalExpression) {
      choices.filter(_.canGenerateRelational)
    } else if (queryContext.needGenerateAggFunction) {
      choices.filter(_.canGenerateAggFunc)
    } else if (queryContext.allowedNestedSubQueryCount <= 0 || checkIfParentIsSelectClause(parent)) {
      choices.filterNot(_ == ExistQuery)
    } else if (queryContext.allowedNestedExpressionCount > 0 && isLast) {
      choices.filter(_.canGenerateNested)
    } else {
      choices
    }
    val finalChoices = filterChoicesForRequiredType[BooleanExpression](
      queryContext, requiredDataType, filteredChoices.toSeq)
    val choice = RandomUtils.nextChoice(finalChoices.toArray)
        .apply(queryContext, parent, requiredDataType, isLast)
    choice
  }

  def checkIfParentIsSelectClause(parent: Option[TreeNode]): Boolean = {
    if (parent.isEmpty) {
      return false
    }
    if (parent.get.isInstanceOf[SelectClause]) {
      return true
    }
    checkIfParentIsSelectClause(parent.get.parent)
  }


  private def choices = {
      Array(LogicalNot, ExistQuery, Predicated, LogicalBinary)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = true

  override def canGenerateAggFunc: Boolean = true

  override def canGenerateNested: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    choices.flatMap(_.possibleDataTypes(querySession)).distinct
  }
}

/**
 * grammar: NOT booleanExpression
 */
class LogicalNot(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    isLast: Boolean) extends BooleanExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val booleanExpression: BooleanExpression = generateBooleanExpression

  private def generateBooleanExpression = {
    BooleanExpression(queryContext, Some(this), BooleanType, isLast)
  }

  override def name: String = s"not_${booleanExpression.name}"

  override def sql: String = s"NOT (${booleanExpression.sql})"

  override def dataType: DataType[_] = BooleanType

  override def isAgg: Boolean = booleanExpression.isAgg

  override def columns: Seq[ColumnReference] = booleanExpression.columns

  override def nonAggColumns: Seq[ColumnReference] = booleanExpression.nonAggColumns
}

/**
 * LogicalNot generator
 */
object LogicalNot extends ExpressionGenerator[LogicalNot] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): LogicalNot = {

    require(requiredDataType == BooleanType, "LogicalNot can only return BooleanType")
    new LogicalNot(querySession, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array(BooleanType)
  }

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false
}

/**
 * Creates an equi-join condition of the form
 *
 * left=(A = B) operator=AND right=(A = B) AND ...
 *
 */

class EquiJoinConditionExpression(
  val queryContext: QueryContext,
  val parent: Option[TreeNode],
  isLast: Boolean) extends BooleanExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val operator: Operator = EQ
  val numJoiningClauses = RandomUtils.choice(1,
    scala.math.max(1, queryContext.allowedNestedExpressionCount))
  val clauses: Seq[ValueExpression] = generateClauses

  private def generateClauses: Seq[ValueExpression] = {
    // Nesting is handled here by adding && clauses. Set the allowed nested expression count to 1.
    val prevNesting = queryContext.allowedNestedExpressionCount
    queryContext.allowedNestedExpressionCount = 1
    val result = (0 until numJoiningClauses).map { i =>
      Comparison(queryContext, Some(this), BooleanType,
        isLast && i == numJoiningClauses - 1, forceEquality = true)
    }
    queryContext.allowedNestedExpressionCount = prevNesting
    result
  }

  override def dataType: DataType[_] = BooleanType
  override def sql: String = clauses.map(clause => s"(${clause.sql})").mkString(" && ")
  override def name: String = clauses.mkString("_")
  override def isAgg: Boolean = clauses.exists(_.isAgg)
  override def columns: Seq[ColumnReference] = clauses.flatMap(_.columns)
  override def nonAggColumns: Seq[ColumnReference] = clauses.flatMap(_.nonAggColumns)
}


/**
 * Generator for equijoin conditions.
 */
object EquiJoinConditionExpression extends ExpressionGenerator[EquiJoinConditionExpression] {
  def apply(
    queryContext: QueryContext,
    parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean): EquiJoinConditionExpression = {
    require(requiredDataType == BooleanType,
      "EquiJoinConditionExpression can only return BooleanType")
    new EquiJoinConditionExpression(queryContext, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array(BooleanType)
  }

  override def canGenerateRelational: Boolean = true
  override def canGenerateNested: Boolean = true
  override def canGenerateAggFunc: Boolean = false
}

/**
 * grammar1: left=booleanExpression operator=AND right=booleanExpression
 * grammar2: left=booleanExpression operator=OR right=booleanExpression
 *
 * Here we combine 2 grammar in one class
 */
class LogicalBinary(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    isLast: Boolean) extends BooleanExpression {

  queryContext.allowedNestedExpressionCount -= 1

  val operator: Operator = RandomUtils.nextChoice(operators)

  val left: BooleanExpression = generateLeft
  val right: BooleanExpression = generateRight

  override def sql: String = s"(${left.sql}) ${operator.op} (${right.sql})"

  override def dataType: DataType[_] = BooleanType

  private def generateLeft: BooleanExpression = {
    BooleanExpression(queryContext, Some(this), BooleanType)
  }

  private def generateRight: BooleanExpression = {
    BooleanExpression(queryContext, Some(this), BooleanType, isLast)
  }

  private def operators = Array(AND, OR)

  override def name: String = s"${left.name}_${operator.name}_${right.name}"

  override def isAgg: Boolean = left.isAgg || right.isAgg

  override def columns: Seq[ColumnReference] = left.columns ++ right.columns

  override def nonAggColumns: Seq[ColumnReference] = left.nonAggColumns ++ right.nonAggColumns
}

/**
 * LogicalBinary generator
 */
object LogicalBinary extends ExpressionGenerator[LogicalBinary] {
  def apply(
    queryContext: QueryContext,
    parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean): LogicalBinary = {
    require(requiredDataType == BooleanType, "LogicalBinary can only return BooleanType")
    new LogicalBinary(queryContext, parent, isLast)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array(BooleanType)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = false
}

/**
 * grammar: valueExpression predicate?
 *
 * Here we have a *predicate?* which means predicate can be None. Hence, `Predicated` can return
 * non-boolean data type. This is one important point during nested expression creation.
 */
class Predicated(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends BooleanExpression {

  // We treat valueExpression IS BETWEEN a AND b as nested expression and valueExpression
  // itself as "Maybe" primitive expression, i.e. it can generate Constant. Also, if required
  // data type is not boolean, we don't use predicate. If we need generate relational expression,
  // don't use predicate as well.
  private val usePredicate =
    !queryContext.needGeneratePrimitiveExpression &&
      !queryContext.needGenerateRelationalExpression &&
      requiredDataType == BooleanType &&
      RandomUtils.nextBoolean()

  if (usePredicate) {
    queryContext.allowedNestedExpressionCount -= 1
  }

  val valueExpression: ValueExpression = generateValueExpression
  val predicateOption: Option[Predicate] = generatePredicateOption

  private def generateValueExpression = {
    val valueExpressionDataType =
      // check relation requirement again. This is a little bit tricky
      if (usePredicate && !queryContext.needGenerateRelationalExpression) {
        RandomUtils.nextChoice(queryContext.dataTypesInAvailableRelations)
      } else {
        requiredDataType
      }
    ValueExpression(queryContext, Some(this), valueExpressionDataType, isLast)
  }

  private def generatePredicateOption = {
    if (usePredicate) {
      Some(Predicate(queryContext, Some(this), valueExpression.dataType))
    } else {
      None
    }
  }
  override def sql: String = s"(${valueExpression.sql}) ${predicateOption.map(_.sql).getOrElse("")}"

  override def dataType: DataType[_] = if (predicateOption.isDefined) {
    BooleanType
  } else {
    valueExpression.dataType
  }

  override def name: String = s"${valueExpression.name}${predicateOption.map("_" + _.name).getOrElse("")}"

  override def isAgg: Boolean = {
    for (x <- predicateOption) {
      if (x.isAgg) {
        return true
      }
    }
    valueExpression.isAgg
  }

  override def columns: Seq[ColumnReference] = valueExpression.columns

  override def nonAggColumns: Seq[ColumnReference] = valueExpression.nonAggColumns
}

/**
 * Predicated generator
 */
object Predicated extends ExpressionGenerator[Predicated] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Predicated = {
    new Predicated(querySession, parent, requiredDataType, isLast)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    (ValueExpression.possibleDataTypes(querySession) :+ BooleanType).distinct
  }

  override def canGenerateRelational: Boolean = true

  override def canGenerateNested: Boolean = true

  override def canGenerateAggFunc: Boolean = true
}

class ExistQuery(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends BooleanExpression {

  private val subQuery = generateSubQuery

  private def generateSubQuery: NestedQuery = {
    NestedQuery(
      QueryContext(availableTables = queryContext.availableTables,
        rqgConfig = queryContext.rqgConfig,
        allowedNestedSubQueryCount = queryContext.allowedNestedSubQueryCount,
        nextAliasId = queryContext.nextAliasId + 1),
      Some(this),
      Some(requiredDataType))
  }

  override def sql: String = s"EXISTS (${subQuery.sql})"

  override def name: String = "subQuery_primary"

  override def dataType: DataType[_] = requiredDataType

  override def isAgg: Boolean = true

  override def columns: Seq[ColumnReference] = Seq.empty

  override def nonAggColumns: Seq[ColumnReference] = Seq.empty
}

/**
 * SubQuery generator
 */
object ExistQuery extends ExpressionGenerator[ExistQuery] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ExistQuery = {
    new ExistQuery(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    DataType.supportedDataTypes(querySession.rqgConfig)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}
