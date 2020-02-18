package org.apache.spark.rqg

import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

case class QueryResult(rows: Seq[Row], schema: StructType)

class SparkConnection(connection: Connection, jdbcUrl: String) {

  def runQuery(query: String): Either[SQLException, QueryResult] = {
    val maybeRs: Either[SQLException, ResultSet] = try {
      Right(connection.prepareStatement(query).executeQuery())
    } catch {
      case e: SQLException => Left(e)
    }
    maybeRs.right.map { rs =>
      val schema: StructType =
        JdbcUtils.getSchema(rs, JdbcDialects.get(jdbcUrl))
      val rows = JdbcUtils.resultSetToRows(rs, schema).toArray.toSeq
      QueryResult(rows, schema)
    }
  }

  def close(): Unit = {
    connection.close()
  }
}

object SparkConnection {

  def openConnection(jdbcUrl: String): SparkConnection = {
    val conn = DriverManager.getConnection(jdbcUrl)
    val settings = Map("spark.sql.crossJoin.enabled" -> true)
    settings.foreach { case (key, value) =>
      conn.prepareStatement(s"SET $key=$value").execute()
    }
    new SparkConnection(conn, jdbcUrl)
  }
}
