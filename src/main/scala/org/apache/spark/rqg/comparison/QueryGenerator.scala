package org.apache.spark.rqg.comparison

import org.apache.hadoop.fs.Path

import org.apache.spark.rqg.ast.{Column, Query, QuerySession, Table}
import org.apache.spark.rqg._
import org.apache.spark.rqg.parser.QueryGeneratorOptions
import org.apache.spark.rqg.runner.SparkSubmitQueryRunner
import org.apache.spark.sql.SparkSession

object QueryGenerator {
  def main(args: Array[String]): Unit = {

    // NOTE: workaround for derby init exception in sbt:
    // java.security.AccessControlException:
    //   access denied org.apache.derby.security.SystemPermission( "engine", "usederbyinternals" )
    System.setSecurityManager(null)

    val options = QueryGeneratorOptions.parse(args)
    RandomUtils.setSeed(options.randomizationSeed)

    val warehouse = new Path(RQGUtils.getBaseDirectory, "warehouse").toString

    val refQueryRunner = new SparkSubmitQueryRunner(
      options.refSparkVersion, options.refSparkHome, options.refMaster)
    val testQueryRunner = new SparkSubmitQueryRunner(
      options.testSparkVersion, options.testSparkHome, options.testMaster)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()

    val tables = describeTables(sparkSession, options.dbName)

    var queryIdx = 0
    while (queryIdx < options.queryCount) {
      val count = math.min(options.queryCount - queryIdx, 100)
      println(s"Generating $count queries to compare")
      val queries = (0 until count)
        .map(_ => Query(
          QuerySession(
            rqgConfig = RQGConfig.load(options.configFile),
            availableTables = tables)))

      println(s"Running queries $queryIdx to ${queryIdx + count - 1} in Reference Spark version")
      val refResult = refQueryRunner.runQueries(queries.map(_.sql))
      println(s"Running queries $queryIdx to ${queryIdx + count - 1} in Test Spark version")
      val testResult = testQueryRunner.runQueries(queries.map(_.sql))
      println(s"Comparing queries $queryIdx to ${queryIdx + count - 1}")

      refResult.zip(testResult).foreach {
        case (refOutput, testOutput) =>
          assert(refOutput.sql == testOutput.sql, "Comparing result between different queries")
          println(s"Comparing query $queryIdx: ${refOutput.sql}")
          if (refOutput.output.contains("Exception") || testOutput.output.contains("Exception")) {
            val errorMessage =
              s"""== Exception occurs while executing query ==
                 |== Expected Answer ==
                 |schema: ${refOutput.schema}
                 |output: ${refOutput.output}
                 |== Actual Answer ==
                 |schema: ${testOutput.schema}
                 |output: ${testOutput.output}
              """.stripMargin
            if (options.stopOnCrash) {
              sys.error(errorMessage)
            } else {
              println(errorMessage)
            }
          } else if (refOutput != testOutput) {
            val errorMessage =
              s"""== Result did not match ==
                 |== Expected Answer ==
                 |schema: ${refOutput.schema}
                 |output: ${refOutput.output}
                 |== Actual Answer ==
                 |schema: ${testOutput.schema}
                 |output: ${testOutput.output}
              """.stripMargin
            if (options.stopOnMismatch) {
              sys.error(errorMessage)
            } else {
              println(errorMessage)
            }
          } else {
            println(s"PASS.")
          }
          queryIdx += 1
      }
    }

    sparkSession.stop()
  }

  def describeTables(sparkSession: SparkSession, dbName: String): Array[Table] = {
    import  sparkSession.implicits._
    sparkSession.sql(s"USE $dbName")
    val tableNames = sparkSession.sql("SHOW TABLES").select("tableName").as[String].collect()
    tableNames.map { tableName =>
      val result = sparkSession.sql(s"DESCRIBE $tableName")
        .select("col_name", "data_type").as[(String, String)].collect()
      val columns = result.map {
        case (columnName, columnType) => Column(tableName, columnName, parseDataType(columnType))
      }
      Table(tableName, columns)
    }
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
