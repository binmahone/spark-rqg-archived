package com.baidu.spark.rqg.ast_new

import com.baidu.spark.rqg.DataType
import com.baidu.spark.rqg.ast_new.relations.RelationPrimary

case class QuerySession(
    var availableTables: Array[Table] = Array.empty,
    var availableRelations: Array[RelationPrimary] = Array.empty,
    var joiningRelation: Option[RelationPrimary] = None,
    var allowedDataTypes: Array[DataType[_]] = DataType.supportedDataTypes,
    var allowedNestedExpressionCount: Int = 5,
    var requiredRelationalExpressionCount: Int = 0,
    var requiredColumnCount: Int = 0,
    var needColumnFromJoiningRelation: Boolean = false,
    var nextAliasId: Int = 0) {

  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }

  def needGenerateRelationalExpression: Boolean = {
    requiredRelationalExpressionCount > 0 &&
      requiredRelationalExpressionCount <= allowedNestedExpressionCount
  }

  def needGeneratePrimitiveExpression: Boolean = {
    allowedNestedExpressionCount <= 0
  }

  def needGenerateColumnExpression: Boolean = {
    needGeneratePrimitiveExpression && requiredColumnCount > 0
  }
}

case class Table(name: String, columns: Array[Column])
case class Column(tableName: String, name: String, dataType: DataType[_])
