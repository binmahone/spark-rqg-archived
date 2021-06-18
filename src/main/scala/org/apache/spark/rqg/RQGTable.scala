package org.apache.spark.rqg

import org.apache.spark.sql.{types => sparktypes}

case class RQGTable(
    dbName: String,
    name: String,
    columns: Seq[RQGColumn],
    provider: String = "parquet",
    warehouseDir: String = "") {

  def schema: sparktypes.StructType = {
    sparktypes.StructType(columns.map(column => sparktypes.StructField(column.name, column.dataType.sparkType)))
  }

  def schemaString: String = {
    columns.map(column => s"${column.name} ${column.dataType.toSql}").mkString(",")
  }

  def prettyString: String = {
    val stringCols = columns.map(column => s"\t${column.name} ${column.dataType.toSql}")
    s"""$dbName (using $provider @ $location)
      |${stringCols.mkString("\n")}
      |""".stripMargin
  }

  def location: String = s"$warehouseDir/$dbName.db/$name"
}
