package org.apache.spark.rqg

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{types => sparktypes}


case class RQGTable(
    dbName: String,
    name: String,
    columns: Seq[RQGColumn],
    provider: String = "hive",
    warehouseDir: String = "") {

  def schema: sparktypes.StructType = {
    sparktypes.StructType(columns.map(column => StructField(column.name, column.dataType.sparkType)))
  }

  def schemaString: String = {
    columns.map(column => {
      if (column.dataType.isInstanceOf[ComplexType[_]]) {
        s"${column.name} ${column.dataType.sparkType.sql}"
      } else {
        s"${column.name} ${column.dataType.typeName}"
      }
    }).mkString(",")
  }

  def location: String = s"$warehouseDir/$dbName.db/$name"
}
