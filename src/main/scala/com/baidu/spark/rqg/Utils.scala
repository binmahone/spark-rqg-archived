package com.baidu.spark.rqg

import com.baidu.spark.rqg.ast.Column
import com.baidu.spark.rqg.ast.relations.RelationPrimary

object Utils {

  private var nextAliasId: Int = 0

  def allowedRelations(
      relations: Array[RelationPrimary],
      allowedDataTypes: Array[DataType[_]]): Array[RelationPrimary] = {
    relations.filter(_.dataTypes.exists(dt => allowedDataTypes.contains(dt)))
  }

  def allowedRelations(
      relations: Array[com.baidu.spark.rqg.ast_new.relations.RelationPrimary],
      allowedDataTypes: Array[DataType[_]]): Array[com.baidu.spark.rqg.ast_new.relations.RelationPrimary] = {
    relations.filter(_.dataTypes.exists(dt => allowedDataTypes.contains(dt)))
  }

  def allowedRelations(
      relations: Array[com.baidu.spark.rqg.ast_new.relations.RelationPrimary],
      allowedDataType: DataType[_]): Array[com.baidu.spark.rqg.ast_new.relations.RelationPrimary] = {
    relations.filter(_.dataTypes.contains(allowedDataType))
  }

  def allowedColumns(
      columns: Array[Column],
      allowedDataTypes: Array[DataType[_]]): Array[Column] = {
    columns.filter(column => allowedDataTypes.contains(column.dataType))
  }

  def allowedColumns(
    columns: Array[com.baidu.spark.rqg.ast_new.Column],
    allowedDataTypes: Array[DataType[_]]): Array[com.baidu.spark.rqg.ast_new.Column] = {
    columns.filter(column => allowedDataTypes.contains(column.dataType))
  }

  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }
}
