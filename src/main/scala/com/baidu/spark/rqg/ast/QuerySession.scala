package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.DataType
import com.baidu.spark.rqg.ast.relations.RelationPrimary

case class QuerySession(
    availableTables: Array[Table] = Array.empty,
    availableRelations: Array[RelationPrimary] = Array.empty,
    joiningRelations: Array[RelationPrimary] = Array.empty,
    allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
    var nestedExpressionCount: Int = 0)

case class Table(name: String, columns: Array[Column])
case class Column(tableName: String, name: String, dataType: DataType[_])
