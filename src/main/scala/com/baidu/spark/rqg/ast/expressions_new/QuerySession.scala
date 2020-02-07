package com.baidu.spark.rqg.ast.expressions_new

import com.baidu.spark.rqg.DataType
import com.baidu.spark.rqg.ast.relations.RelationPrimary

case class QuerySession(
  var availableTables: Array[Table] = Array.empty,
  var availableRelations: Array[RelationPrimary] = Array.empty,
  var joiningRelations: Array[RelationPrimary] = Array.empty,
  var allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
  var allowedNestedExpressionCount: Int = 0)

case class Table(name: String, columns: Array[Column])
case class Column(tableName: String, name: String, dataType: DataType[_])
