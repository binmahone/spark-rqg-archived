package com.baidu.spark.rqg

case class RQGTable(name: String, columns: Seq[RQGColumn], location: String = "") {
  def schema: String = {
    columns.map { column =>
      s"${column.name} ${column.dataType.typeName}"
    }.mkString(",")
  }
}
