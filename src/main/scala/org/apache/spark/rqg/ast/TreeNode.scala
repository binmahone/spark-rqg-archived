package org.apache.spark.rqg.ast

import org.apache.spark.rqg.{DataType, RQGConfig}
import org.apache.spark.rqg.ast.relations.RelationPrimary
import com.typesafe.config.ConfigFactory

/**
 * A TreeNode represents a part of a Query.
 */
trait TreeNode {
  def parent: Option[TreeNode]
  def querySession: QuerySession
  def sql: String
}

/**
 * QuerySession contains all the states during a Query generating, such as:
 * 1. available tables a FromClause can choose from
 * 2. aliasId to generate unique alias
 * 3. allowed data types when generating an expression
 */
case class QuerySession(
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
