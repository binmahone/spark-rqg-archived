package com.baidu.spark.rqg

import java.sql.{DriverManager, Connection}

class SparkConnection(connection: Connection) {

  def runQuery(query: String): Unit = {
    // TODO: error handling
    connection.prepareStatement(query).executeQuery()
  }

  def createTable(table: RQGTable): Unit = {
    val dropSql = s"DROP TABLE IF EXISTS ${table.name}"
    runQuery(dropSql)
    val sql = s"CREATE TABLE ${table.name} (${table.schema}) " +
      // s"USING ${table.format} " +
      s"LOCATION '${table.location}'"
    runQuery(sql)
  }
}

object SparkConnection {

  def openConnection(jdbcUrl: String): SparkConnection = {
    val conn = DriverManager.getConnection(jdbcUrl)
    new SparkConnection(conn)
  }
}
