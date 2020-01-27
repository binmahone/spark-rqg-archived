package com.baidu.spark.rqg

case class RQGTable(
    dbName: String,
    name: String,
    columns: Seq[RQGColumn],
    provider: String = "hive",
    warehouseDir: String = "") {
  def schema: String = {
    columns.map { column =>
      s"${column.name} ${column.dataType.typeName}"
    }.mkString(",")
  }

  def location: String = s"$warehouseDir/$dbName.db/$name"
}
