package com.baidu.spark.rqg

import java.sql.{DriverManager, Connection}

class SparkConnection(connection: Connection) {

  def runQuery(query: String): Unit = {
    // TODO: error handling
    connection.prepareStatement(query).executeQuery()
  }
}

object SparkConnection {

  def openConnection(jdbcUrl: String): SparkConnection = {
    val conn = DriverManager.getConnection(jdbcUrl)
    new SparkConnection(conn)
  }
}
