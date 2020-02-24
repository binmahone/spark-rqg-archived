package org.apache.spark.rqg

import org.apache.spark.sql.types.{StructField, StructType}

case class RQGTable(
    dbName: String,
    name: String,
    columns: Seq[RQGColumn],
    provider: String = "hive",
    warehouseDir: String = "") {

  def schema: StructType = {
    StructType(columns.map(column => StructField(column.name, column.dataType.sparkType)))
  }

  def schemaString: String = {
    columns.map { column =>
      s"${column.name} ${column.dataType.typeName}"
    }.mkString(",")
  }

  def location: String = s"$warehouseDir/$dbName.db/$name"
}
