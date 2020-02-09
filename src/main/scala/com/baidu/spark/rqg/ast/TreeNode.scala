package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.DataType
import com.baidu.spark.rqg.ast.relations.RelationPrimary

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
    var availableTables: Array[Table] = Array.empty,
    var availableRelations: Array[RelationPrimary] = Array.empty,
    var joiningRelation: Option[RelationPrimary] = None,
    var allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
    var allowedNestedExpressionCount: Int = 5,
    var nextAliasId: Int = 0) {
  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }

  def needGeneratePrimitiveExpression: Boolean = {
    allowedNestedExpressionCount <= 0
  }

  def dataTypesInAvailableRelations: Array[DataType[_]] = {
    allowedDataTypes.intersect(availableRelations.flatMap(_.columns).map(_.dataType)).distinct
  }

  def relationsBasedOnAllowedDataType: Array[RelationPrimary] = {
    availableRelations.filter(_.dataTypes.exists(dt => allowedDataTypes.contains(dt)))
  }
}

/**
 * Represents a table from db
 */
case class Table(name: String, columns: Array[Column])

/**
 * Represents a column from db
 */
case class Column(tableName: String, name: String, dataType: DataType[_])