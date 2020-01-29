package com.baidu.spark.rqg

import com.baidu.spark.rqg.ast.{Query, QuerySession}

class QueryGenerator(tables: Array[RQGTable]) {

  def createQuery(): String = {
    new Query(QuerySession(tables = tables)).toSql
  }
}

object QueryGenerator {

  val sparkConnection: SparkConnection =
    SparkConnection.openConnection("jdbc:hive2://localhost:10000")

  def main(args: Array[String]): Unit = {
    println(new QueryGenerator(describeTables("rqg_test_db")).createQuery())
  }

  def describeTables(dbName: String): Array[RQGTable] = {
    sparkConnection.runQuery(s"use $dbName")
    val tableNames = sparkConnection.runQuery("show tables") match {
      case Right(result) => result.rows.map(row => row.getString(1))
      case Left(err) => throw new Exception(s"Error show tables", err)
    }
    tableNames.map { tableName =>
      sparkConnection.runQuery(s"DESCRIBE $tableName") match {
        case Right(result) =>
          val columns = result.rows.map { row =>
            val columnName = row.getString(0)
            val columnType = parseDataType(row.getString(1))
            RQGColumn(columnName, columnType)
          }
          RQGTable(dbName, tableName, columns)
        case Left(err) => throw new Exception(s"Error show tables", err)
      }
    }.toArray
  }

  private def parseDataType(dataType: String): DataType[_] = {
    val decimalPattern = "decimal\\(([0-9]+),([0-9]+)\\)".r
    dataType match {
      case "boolean" => BooleanType
      case "tinyint" => TinyIntType
      case "smallint" => SmallIntType
      case "int" => IntType
      case "bigint" => BigIntType
      case "float" => FloatType
      case "double" => DoubleType
      case "string" => StringType()
      case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    }
  }
}