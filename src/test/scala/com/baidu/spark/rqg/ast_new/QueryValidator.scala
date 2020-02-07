package com.baidu.spark.rqg.ast_new

import org.apache.spark.sql.SparkSession


class QueryValidator(tables: Array[Table]) {

  private val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  init()

  def init(): Unit = {
    tables.foreach { table =>
      val schema = table.columns.map { column =>
        s"${column.name} ${column.dataType.typeName}"
      }.mkString(",")
      sparkSession.sql(s"CREATE TABLE ${table.name} ($schema) USING PARQUET")
    }
  }

  def assertValid(query: String): Unit = {
    sparkSession.sql(query).queryExecution.assertAnalyzed()
  }
}
