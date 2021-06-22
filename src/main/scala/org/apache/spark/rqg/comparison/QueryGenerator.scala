package org.apache.spark.rqg.comparison

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import org.apache.spark.rqg.ast.{Column, Query, QueryContext, Table}
import org.apache.spark.rqg._
import org.apache.spark.rqg.parser.QueryGeneratorOptions
import org.apache.spark.rqg.runner.{RQGQueryRunnerApp, SparkSubmitQueryRunner, SparkSubmitUtils}
import org.apache.spark.sql.SparkSession
import org.hibernate.jdbc.util.BasicFormatterImpl

object QueryGenerator extends Runner {
  // Must be lazy so Spark doesn't use Log4J before we initialize it.
  lazy val warehouse: String = {
    new Path(RQGUtils.getBaseDirectory, "warehouse").toString
  }

  // Must be lazy so Spark doesn't use Log4J before we initialize it.
  private lazy val sparkSession: SparkSession = {
    // NOTE: workaround for derby init exception in sbt:
    // java.security.AccessControlException:
    //   access denied org.apache.derby.security.SystemPermission( "engine", "usederbyinternals" )
    System.setSecurityManager(null)
    SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val options = QueryGeneratorOptions.parse(args)
    val rqgConfig = RQGConfig.load(options.configFile)
    RandomUtils.setSeed(options.randomizationSeed)
    logInfo(
      s"""
         |Spark RQG
         |  Output directory: ${outputDirFile.toString}
         |  Log4J output: ${log4jFile.toString}
         |  Random Seed: ${options.randomizationSeed}
         |  Number of Queries: ${options.queryCount}
         |  Queries per spark-submit: ${options.batchSize}
         |  Dry Run: ${options.dryRun}
         |""".stripMargin)

    val refQueryRunner = new SparkSubmitQueryRunner(
      options.refSparkVersion, options.refSparkHome, options.refMaster, verbose = options.verbose)
    val testQueryRunner = new SparkSubmitQueryRunner(
      options.testSparkVersion, options.testSparkHome, options.testMaster, verbose = options.verbose)

    val tables = describeTables(sparkSession, options.dbName)
    if (options.dryRun) {
      // Generate and print the queries, and exit.
      generateQueries(options.queryCount, rqgConfig, tables, dryRun = true)
      sparkSession.stop()
      return
    }

    if (options.skipDbSetup) {
      logInfo(s"====== Skipping Setup of Database for Reference and Test Spark =======")
      logInfo(s"Database: '${options.dbName}' at $warehouse")
    } else {
      logInfo(s"====== Setting up Database for Reference and Test Spark =======")
      logInfo(s"Database: '${options.dbName}' at $warehouse")
      val dbSetupQueries = tables.flatMap(
        table => Seq(
          toDropTableSql(options.dbName, table),
          toCreateTableSql(options.dbName, warehouse, table, options.useParquet)
        ))

      logInfo(s"--- Setting up in reference ---")
      refQueryRunner.runQueries(dbSetupQueries,
        new Path(new Path(outputDirFile.toString), "prepare-db"), "reference")
      logInfo(s"--- Setting up in test ---")
      testQueryRunner.runQueries(dbSetupQueries,
        new Path(new Path(outputDirFile.toString), "prepare-db"), "test")
    }

    // The manifest file, which holds the result of each query.
    val manifestFile = new File(outputDirFile, "manifest.txt")
    val manifestFileStream = new PrintWriter(manifestFile)

    var queryIdx = 0
    var batchIdx = 0
    // Stats across all queries
    var numPassed = 0
    var numFailed = 0
    var numSkipped = 0
    var numMismatched = 0
    var numProducedData = 0

    // For logging exit.
    def logEndState() = {
      logInfo(
        s"""
           |Finished executing ${queryIdx} queries.
           |
           |$numPassed queries passed
           |  - $numProducedData passing queries produced meaningful data
           |$numMismatched queries produced incorrect results compared to reference
           |$numFailed queries crashed or threw exceptions
           |$numSkipped queries skipped due to crash in batch
           |
           |Full manifest written to ${manifestFile.toString}
           |""".stripMargin)
    }

    // Run each query with the given configurations.
    while (queryIdx < options.queryCount) {
      //  Directory where information for this batch will be written.
      val batchDir = new Path(new Path(outputDirFile.toString), s"batch-$batchIdx")
      val numQueriesThisBatch = scala.math.min(options.queryCount - queryIdx, options.batchSize)

      logInfo(s"====== Batch $batchIdx - Generating $numQueriesThisBatch queries " +
        s"($queryIdx-${queryIdx + numQueriesThisBatch} out of ${options.queryCount} total) =======")
      logInfo(s"Logging all batch info at $batchDir")

      val queries = generateQueries(numQueriesThisBatch, rqgConfig, tables)
      val queryFile = new Path(batchDir, "queries.txt")
      SparkSubmitUtils.stringToFile(queries.mkString("\n"), Some(queryFile))
      logInfo(s"Wrote queries for batch $batchIdx to $queryFile")

      logInfo(s"--- Running reference ---")
      val (refResult, refLogFile) = refQueryRunner.runQueries(
        queries, batchDir, "reference", rqgConfig.getReferenceSparkConfig)
      assert(refResult.size == numQueriesThisBatch, s"${refResult.size}, $numQueriesThisBatch")
      if (refResult.exists(_.output == "CRASH")) {
        logInfo(
          s"""!!!! CRASHED!
             |!!!! All queries for this batch: $queryFile
             |!!!! Spark Log4j output for crashed run: ${refLogFile.toString}
             |""".stripMargin)
      }

      logInfo(s"--- Running test ---")
      val (testResult, testLogFile) = testQueryRunner.runQueries(
        queries, batchDir, "test", rqgConfig.getTestSparkConfig)
      assert(refResult.size == numQueriesThisBatch)
      if (testResult.exists(_.output == "CRASH")) {
        logInfo(
          s"""!!!! CRASHED!
             |!!!! All queries for this batch: $queryFile
             |!!!! Spark Log4j output for crashed run: ${testLogFile.toString}
             |""".stripMargin)
      }

      // Compare results and write out to manifest file.
      logInfo(s"Comparing reference results to test results")
      var sawCrash = false
      var sawMismatch = false
      refResult.zip(testResult).zipWithIndex.foreach { case ((refOutput, testOutput), i) =>
        val (errorMessage, status) = compareQueries(refOutput, testOutput)

        // Update state.
        sawCrash |= (status == "CRASH")
        sawMismatch |= (status == "MISMATCH")

        // Counts as producing data if the query passed and the output was empty.
        val producedData = refOutput.output.trim == "" &&
          testOutput.output.trim == "" && status == "PASS"
        if (producedData) {
          numProducedData += 1
        }

        status match {
          case "CRASH" | "EXCEPTION" =>
            numFailed += 1
          case "MISMATCH" =>
            numMismatched += 1
          case "SKIPPED" =>
            numSkipped += 1
          case _ =>
            numPassed += 1
        }

        // Write to manifest and log to console.
        writeToManifestAndLog(
          manifestFileStream,
          status,
          producedData,
          batchIdx,
          queryIdx + i,
          queries(i),
          refLogFile.toString,
          testLogFile.toString,
          errorMessage
        )
      }

      queryIdx += numQueriesThisBatch
      batchIdx += 1

      if (options.stopOnCrash && sawCrash) {
        logEndState()
        logInfo("!!!! Exiting -- saw a crash.")
        sys.exit(0)
      } else if (options.stopOnMismatch && sawMismatch) {
        logEndState()
        logInfo("!!!! Exiting -- saw a query result mismatch.")
        sys.exit(0)
      }
    }

    logEndState()
    sparkSession.stop()
  }

  /**
   * Compares the reference output to the test output and returns an optional error message and
   * status ("PASS", "CRASH", "EXCEPTION", "MISMATCH", or "SKIPPED")
   */
  private def compareQueries(
    refOutput: RQGQueryRunnerApp.QueryOutput,
    testOutput: RQGQueryRunnerApp.QueryOutput): (Option[String], String) = {
    assert(refOutput.sql.trim() == testOutput.sql.trim(),
      s"query input for comparison does not match (${refOutput.sql} vs ${testOutput.sql})")

    val (headerMessage, status) = (refOutput.output, testOutput.output) match {
      case ("CRASH", _) | (_, "CRASH") =>
        (Some("Crash occurs while executing query"), "CRASH")
      case ("SKIPPED", _) | (_, "SKIPPED") =>
        (Some("Query skipped due to crash while executing different query"), "SKIPPED")
      case (ref, test) if ref.contains("Exception") || test.contains("Exception") =>
        (Some("Exception occurs while executing query"), "EXCEPTION")
      case (ref, test) if ref != test =>
        (Some("Result did not match"), "MISMATCH")
      case _ => (None, "PASS")
    }

    val errorMessage = headerMessage.map { header =>
      s"""== $header ==
         |== Expected Answer ==
         |schema: ${refOutput.schema}
         |output: ${refOutput.output}
         |== Actual Answer ==
         |schema: ${testOutput.schema}
         |output: ${testOutput.output}""".stripMargin
    }
    (errorMessage, status)
  }

  lazy val formatter = new BasicFormatterImpl()

  def formattedSql(sql: String): String = formatter.format(sql)

  private def writeToManifestAndLog(
    manifestFileStream: PrintWriter,
    status: String,
    producedData: Boolean,
    batchIdx: Int,
    queryIdx: Int,
    querySql: String,
    refLog4jFile: String,
    testLog4jFile: String,
    errorMsgOpt: Option[String]) = {
    val msg =
      s"""
         |----! Query Status for $sessionName/batch-$batchIdx/$queryIdx - $status
         |Reference Spark Log4J: $refLog4jFile
         |Test Spark Log4J: $testLog4jFile
         |Query Produced Data: $producedData
         |----! Query SQL
         |${formattedSql(querySql).trim}
         |----! Error Message
         |${errorMsgOpt.getOrElse("")}
         |""".stripMargin
    manifestFileStream.write(msg)
    // Always flush, since this is the source of truth for what the RQG did.
    manifestFileStream.flush()

    // Log failures to stderr too.
    if (status != "PASS") {
      logInfo(msg)
    } else {
      logInfo(s"Query $queryIdx passed.")
    }
  }

  private def generateQueries(
    numQueries: Int,
    rqgConfig: RQGConfig,
    tables: Array[Table],
    dryRun: Boolean = false): Array[String] = {
    val queries = ArrayBuffer[String]()
    var failedCount = 0
    while (queries.size < numQueries && failedCount < numQueries * 100) {
      try {
        val querySQL = Query(QueryContext(rqgConfig = rqgConfig, availableTables = tables)).sql
        // Assert to make sure we generate valid SQL query.
        val queryDf = sparkSession.sql(querySQL)
        queryDf.queryExecution.assertAnalyzed()
        if (dryRun) {
          logInfo(
            s"""
               |schema: ${queryDf.schema.sql}
               |----! Query ${queries.size}:${formattedSql(querySQL)}
               |""".stripMargin)
        }
        queries += querySQL
      } catch {
        case e: RQGEmptyChoiceException =>
          logDebug(e.toString)
          failedCount += 1
      }
    }
    logDebug(s"Failed $failedCount times while generating queries")
    if (queries.size != numQueries) {
      sys.error("Failed too many times while generating queries")
    }
    queries.toArray
  }

  private def toDropTableSql(dbName: String, table: Table): String = {
    s"DROP TABLE IF EXISTS $dbName.${table.name}"
  }

  /**
   * Creates a CREATE TABLE query so Spark can see the table created by
   * [[org.apache.spark.rqg.comparison.DataGenerator]].
   */
  private def toCreateTableSql(
      dbName: String,
      warehouse: String,
      table: Table,
      forceParquet: Boolean): String = {
    s"CREATE TABLE $dbName.${table.name} (${table.schemaString}) " +
        (if (forceParquet) "USING PARQUET " else "") +
      s"LOCATION '$warehouse/$dbName.db/${table.name}'"
  }

  private def describeTables(sparkSession: SparkSession, dbName: String): Array[Table] = {
    import  sparkSession.implicits._
    sparkSession.sql(s"USE $dbName")
    val tableNames = sparkSession.sql("SHOW TABLES").select("tableName").as[String].collect()
    tableNames.map { tableName =>
      val result = sparkSession.sql(s"DESCRIBE $tableName")
        .select("col_name", "data_type").as[(String, String)].collect()
      val columns = result.map {
        case (columnName, columnType) => {
          Column(tableName, columnName, parseDataType(columnType))
        }
      }
      val tbl = Table(tableName, columns)
      tbl
    }
  }

  private def parseDataType(dataType: String): DataType[_] = {
    val decimalPattern = "decimal\\(([0-9]+),([0-9]+)\\)".r
    val arrayPattern = "array<(.*)>$".r
    val mapPattern1= "^map<(\\w*),(\\w*)>$".r
    val mapPattern2 = "^map<(\\w*),(\\w*<.*>)>$".r
    val mapPattern3 = "^map<(\\w*<.*?>),(\\w*)>$".r
    val mapPattern4 = "^map<(\\w*<.*?>),(\\w*<.*>)>$".r
    val structPattern = "struct<(.*)>$".r
    // Case 1
    // map<int,string>
    // ^map<(\w*),(\w*)>$
    // Case 2
    // map<string,map<array<int>,int>>
    // ^map<(\w*),(\w*<.*>)>$
    // Case 3
    // map<array<integer>,string>
    // ^map<(\w*<.*>),(\w*)>$
    // Case 4
    // map<array<int>,map<array<int>,int>>
    // ^map<(\w*<.*>),(\w*<.*>)>$
    dataType match {
      case "boolean" => BooleanType
      case "tinyint" => TinyIntType
      case "smallint" => SmallIntType
      case "int" => IntType
      case "bigint" => BigIntType
      case "float" => FloatType
      case "double" => DoubleType
      case "string" => StringType
      case "date" => DateType
      case "timestamp" => TimestampType
      case arrayPattern(inner) =>
        ArrayType(parseDataType(inner))
      case mapPattern1(keyType, valueType) =>
        MapType(parseDataType(keyType), parseDataType(valueType))
      case mapPattern2(keyType, valueType) =>
        MapType(parseDataType(keyType), parseDataType(valueType))
      case mapPattern3(keyType, valueType) =>
        MapType(parseDataType(keyType), parseDataType(valueType))
      case mapPattern4(keyType, valueType) =>
        MapType(parseDataType(keyType), parseDataType(valueType))
      case structPattern(inner) =>
        val s = inner + ","
        var i = 0
        var j = 0
        var count = 0
        val split = ArrayBuffer[String]()
        while (j < s.length) {
          if (s.charAt(j) == '<') {
            count += 1
          } else if (s.charAt(j) == '>') {
            count -= 1
          } else if (s.charAt(j) == ',') {
            if (count == 0) {
              split += s.substring(i, j)
              i = j + 1
            }
          }
          j += 1
        }
        val arr = split.map(x => x.replaceFirst("\\w*:", "")).toArray
        val fields = arr.zipWithIndex.map { case (field, index) =>
          val parsedType = parseDataType(field)
          StructField(parsedType.fieldName + index, parsedType)
        }
        StructType(fields)
      case decimalPattern(precision, scale) => DecimalType(precision.toInt, scale.toInt)
    }
  }
}
