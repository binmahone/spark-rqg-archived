package org.apache.spark.rqg.ast

import org.apache.spark.rqg.ComplexType
import org.apache.spark.sql.SparkSession

class QueryValidator(querySession: QueryContext) {

  private val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  init()

  def init(): Unit = {
    querySession.availableTables.foreach { table =>
      val schema = table.columns.map { column =>
        if (column.dataType.isInstanceOf[ComplexType[_]]) {
          s"${column.name} ${column.dataType.sparkType.sql}"
        } else {
          s"${column.name} ${column.dataType.typeName}"
        }
      }.mkString(",")
      sparkSession.sql(s"CREATE TABLE ${table.name} ($schema) USING PARQUET")
    }
  }

  def assertValid(query: String): Unit = {
    println(query)
    sparkSession.sql(query).queryExecution.assertAnalyzed()
  }
}
