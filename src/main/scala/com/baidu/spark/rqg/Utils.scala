package com.baidu.spark.rqg

import com.baidu.spark.rqg.ast.{Column, RelationPrimary}

object Utils {

  private var nextAliasId: Int = 0

  def allowedRelations(
      relations: Array[RelationPrimary],
      allowedDataTypes: Array[DataType[_]]): Array[RelationPrimary] = {
    relations.filter(_.dataTypes.exists(dt => allowedDataTypes.contains(dt)))
  }

  def allowedColumns(
      columns: Array[Column],
      allowedDataTypes: Array[DataType[_]]): Array[Column] = {
    columns.filter(column => allowedDataTypes.contains(column.dataType))
  }

  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }
}
