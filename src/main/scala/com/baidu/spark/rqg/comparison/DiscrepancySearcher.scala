package com.baidu.spark.rqg.comparison

import com.baidu.spark.rqg._
import com.baidu.spark.rqg.ast.{Column, Query, QuerySession, Table}
import com.baidu.spark.rqg.parser.DiscrepancySearcherOptions

import org.apache.spark.internal.Logging

object DiscrepancySearcher extends Logging {
  def main(args: Array[String]): Unit = {

    val options = DiscrepancySearcherOptions.parse(args)

    val refConnection =
      SparkConnection.openConnection(s"jdbc:hive2://${options.refHost}:${options.refPort}")
    val testConnection =
      SparkConnection.openConnection(s"jdbc:hive2://${options.testHost}:${options.testPort}")

    val refTables = describeTables(refConnection, options.dbName)
    val testTables = describeTables(testConnection, options.dbName)

    val commonTables = refTables.filter(t => testTables.exists(_.sameTable(t)))
    val queries = (0 until options.queryCount)
      .map(_ => Query(
        QuerySession(
          rqgConfig = RQGConfig.load(options.configFile),
          availableTables = commonTables)))

    val comparator = ResultComparator(tolerance = 0.000000001f)
    try {
      queries.foreach { query =>
        logDebug(query.sql)
        val refResult = refConnection.runQuery(query.sql) match {
          case Right(res) => res
          case Left(exc) => throw exc
        }
        val testResult = testConnection.runQuery(query.sql) match {
          case Right(res) => res
          case Left(exc) => throw exc
        }
        comparator.checkAnswer(refResult.rows, testResult.rows).foreach { err =>
          sys.error(
            s"""
               |Query results did not match:
               | ${query.sql}
               | ----
               |$err
             """.stripMargin
          )
        }
      }
    } finally {
      refConnection.close()
      testConnection.close()
    }
  }

  def describeTables(sparkConnection: SparkConnection, dbName: String): Array[Table] = {
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
            Column(tableName, columnName, columnType)
          }.toArray
          Table(tableName, columns)
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
      case "string" => StringType
      case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    }
  }
}