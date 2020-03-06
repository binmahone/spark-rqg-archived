package org.apache.spark.rqg.ast

import org.apache.spark.rqg.{DataType, RQGConfig}
import org.apache.spark.rqg.ast.relations.RelationPrimary
import com.typesafe.config.ConfigFactory

/**
 * A TreeNode represents a part of a Query.
 * A set of TreeNode will form an AST and represent a complete Query. Here is an example:
 *
 *                          -------- Query --------------
 *                         /                             \
 *               SelectClause                           FromClause
 *            /           \                           /           \
 * SetQuantifier  NamedExpression                  TablePrimary   JoinRelation
 *     |          /        \ <-> create as a child
 * "DISTINCT"  Alias    BooleanExpression (trait)
 *             /            \ <-> random choose a sub-class
 *        "expr_1"       Predicated (class extends BooleanExpression)
 *                           \ <-> create as a child
 *                        ValueExpression (trait)
 *                            \ <-> random choose a sub-class
 *                         PrimaryExpression (trait extends ValueExpression)
 *                             \ <-> random choose a sub-class
 *                          Column (class extends PrimaryExpression)
 *
 * This example contains 2 kinds of node creation:
 * 1. parent node create a child node, e.g. NamedExpression create a BooleanExpression
 * 2. abstract node create a sub-class node, e.g. PrimaryExpression create a Column
 *
 * TODO: add a check to make sure the AST is consistent to target spark version. (maybe comparing
 * the commit id of sqlbase.g4)
 */
trait TreeNode {
  def parent: Option[TreeNode]
  def queryContext: QueryContext
  def sql: String
}

/**
 * QueryContext contains all the states and requirements during a Query generating, such as:
 * 1. available tables a FromClause can choose from
 * 2. aliasId to generate unique alias
 * 3. allowed data types when generating an expression
 * 4. required nested expression count
 */
case class QueryContext(
    var rqgConfig: RQGConfig = RQGConfig.load(),
    var availableTables: Array[Table] = Array.empty,
    var availableRelations: Array[RelationPrimary] = Array.empty,
    var joiningRelation: Option[RelationPrimary] = None,
    var allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
    var allowedNestedExpressionCount: Int = 5,
    var requiredRelationalExpressionCount: Int = 0,
    var requiredColumnCount: Int = 0,
    var needColumnFromJoiningRelation: Boolean = false,
    var aggPreference: Int = AggPreference.FORBID,
    var nextAliasId: Int = 0) {
  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }

  def needGenerateRelationalExpression: Boolean = {
    requiredRelationalExpressionCount > 0 &&
      requiredRelationalExpressionCount == allowedNestedExpressionCount
  }

  def needGeneratePrimitiveExpression: Boolean = {
    allowedNestedExpressionCount <= 0
  }

  def needGenerateColumnExpression: Boolean = {
    needGeneratePrimitiveExpression && requiredColumnCount > 0
  }

  def needGenerateAggFunction: Boolean = {
    aggPreference == AggPreference.PREFER && allowedNestedExpressionCount == 1
  }

  def dataTypesInAvailableRelations: Array[DataType[_]] = {
    allowedDataTypes.intersect(availableRelations.flatMap(_.columns).map(_.dataType)).distinct
  }

  def commonDataTypesForJoin: Array[DataType[_]] = {
    joiningRelation.map(_.columns.map(_.dataType))
      .map(_.intersect(dataTypesInAvailableRelations))
      .getOrElse(dataTypesInAvailableRelations)
      .distinct
  }

  def relationsBasedOnAllowedDataType: Array[RelationPrimary] = {
    availableRelations.filter(_.dataTypes.exists(dt => allowedDataTypes.exists(dt.sameType)))
  }
}

/**
 * Represents a table from db
 */
case class Table(name: String, columns: Array[Column]) {
  def sameTable(other: Table): Boolean = {
    name == other.name && columns.sameElements(other.columns)
  }
}

/**
 * Represents a column from db
 */
case class Column(tableName: String, name: String, dataType: DataType[_])

object AggPreference {
  val PREFER = 0
  val ALLOW = 1
  val FORBID = 2
}
