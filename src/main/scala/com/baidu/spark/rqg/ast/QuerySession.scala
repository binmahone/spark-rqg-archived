package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.RQGTable

case class QuerySession(
  tables: Array[RQGTable] = Array.empty,
  selectedTables: Array[RelationPrimary] = Array.empty,
  joiningTables: Array[RelationPrimary] = Array.empty,
  var aliasId: Int = 0) {

  def nextAliasId: Int = {
    val id = aliasId
    aliasId += 1
    id
  }
}
