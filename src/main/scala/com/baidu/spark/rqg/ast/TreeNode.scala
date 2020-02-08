package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.DataType

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
case class QuerySession(var availableTables: Array[Table] = Array.empty)

/**
 * Represents a table from db
 */
case class Table(name: String, columns: Array[Column])

/**
 * Represents a column from db
 */
case class Column(tableName: String, name: String, dataType: DataType[_])