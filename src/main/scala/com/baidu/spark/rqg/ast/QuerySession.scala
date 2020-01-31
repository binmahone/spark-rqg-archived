package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{DataType, RQGTable}

case class QuerySession(
  tables: Array[RQGTable] = Array.empty,
  availableRelations: Array[RelationPrimary] = Array.empty,
  primaryRelations: Array[RelationPrimary] = Array.empty,
  joiningRelations: Array[RelationPrimary] = Array.empty,
  allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
  var aliasId: Int = 0,
  var nestedExpressionCount: Int = 0) {

  def nextAliasId: Int = {
    val id = aliasId
    aliasId += 1
    id
  }
}
