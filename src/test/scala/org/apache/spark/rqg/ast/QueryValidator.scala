package org.apache.spark.rqg.ast

import org.apache.spark.sql.SparkSession

class QueryValidator(querySession: QuerySession) {

  private val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  init()

  def init(): Unit = {
    querySession.availableTables.foreach { table =>
      val schema = table.columns.map { column =>
        s"${column.name} ${column.dataType.typeName}"
      }.mkString(",")
      sparkSession.sql(s"CREATE TABLE ${table.name} ($schema) USING PARQUET")
    }
  }

  def assertValid(query: String): Unit = {
    println(query)
    sparkSession.sql(query).queryExecution.assertAnalyzed()
  }
}
