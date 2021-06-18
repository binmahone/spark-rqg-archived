package org.apache.spark.rqg.runner

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rqg.runner.RQGQueryRunnerApp.QueryOutput

/**
 * Runs Spark queries using `spark-submit`.
 */
class SparkSubmitQueryRunner(
    version: String,
    sparkHomeOpt: Option[String] = None,
    master: String = "local[*]",
    timeout: Int = 0,
    verbose: Boolean = false) extends SparkQueryRunner with Logging {

  private val sparkTestingDir = new File("/tmp/test-spark")
  private val QUERY_COMPLETED_PATTERN = "\tQuery ([0-9]+) completed.".r

  override def runQueries(
    queries: Seq[String],
    rootOutputDir: Path,
    pathSuffix: String = "",
    extraSparkConf: Map[String, String] = Map.empty): (Seq[QueryOutput], Path) = {

    val outputDir = new Path(rootOutputDir, s"${version}_output_$pathSuffix")

    val sparkHome = sparkHomeOpt
      .map(new File(_))
      .getOrElse(new File(sparkTestingDir, s"spark-$version"))

    // No Spark at the directory we chose. Try downloading it.
    if (!sparkHome.exists()) {
      logInfo(s"SPARK_HOME='$sparkHome' not found -- downloading Spark to temp directory")
      SparkSubmitUtils.tryDownloadSpark(version, sparkTestingDir.getCanonicalPath)
    }

    val jarFile = SparkSubmitUtils.createSparkAppJar(
      Seq(RQGQueryRunnerApp.getClass, SparkSubmitUtils.getClass))

    val queryFile = SparkSubmitUtils.stringToFile(queries.mkString("\n"), None)

    // Path where the logging file is written.
    val log4jFilePath = new Path(outputDir, "log4j.log")
    val log4jFileString = SparkSubmitQueryRunner.getLog4JFileString("INFO", log4jFilePath)

    // log4j properties file for Spark. This points Spark's logging to the file above.
    val log4jPropertiesFile = SparkSubmitUtils.stringToFile(log4jFileString, None)
    val resultFile = new Path(outputDir, "resultFile-%s.txt".format(System.currentTimeMillis()))

    val sparkArgs = Seq(
      "--class", RQGQueryRunnerApp.getClass.getName.stripSuffix("$"),
      "--name", s"Spark $version RQG Runner",
      "--master", master,
      "--driver-java-options", s"-Dlog4j.configuration=file://${log4jPropertiesFile.toString}",
      "--files", s"${queryFile.toString}"
    )

    val configArgs = extraSparkConf.flatMap(e => Seq("--conf", e._1 + "=" + e._2))
    val appArgs = Seq(jarFile.toString, queryFile.getName, resultFile.toString, verbose.toString)

    // Keep track of the last known query.
    var lastKnownSuccessfulQuery = -1
    val args = sparkArgs ++ configArgs ++ appArgs
    val exitCode = SparkSubmitUtils.runSparkSubmit(args, sparkHome.getCanonicalPath, timeout) {
      // Watch stdout for signals on progress. This will help detect which query, if any, crashes.
      line =>
        logInfo(line)
        line match {
          case QUERY_COMPLETED_PATTERN(queryIdx) =>
            lastKnownSuccessfulQuery = queryIdx.toInt
          case _ =>
        }
    }

    // Read the result into a string. Note that we may read a partial result file if there
    // was a crash.
    val resultStream = new FileReader(resultFile.toString)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        resultStream.read() match {
          case -1 =>
            reading = false
          case byte =>
            outStream.write(byte)
        }
      }
      outStream.flush()
    } finally {
      resultStream.close()
    }
    val outputString = new String(outStream.toByteArray)

    // Convert the output string into a `QueryOutput` list.
    val queryOutputs: Seq[QueryOutput] = {
      val segments = outputString.split("-- !query.+\n")
      // If the exit code was 0, we should have received a full output file.
      val numSuccessfulQueries = if (exitCode == 0) {
        // each query has 3 segments, plus the header
        assert(segments.size == queries.size * 3 + 1,
          s"Expected ${queries.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            s"Try regenerate the result files.")
        queries.size
      } else {
        val numSuccessfulQueries = lastKnownSuccessfulQuery + 1
        assert(segments.size >= numSuccessfulQueries * 3 + 1,
          s"Expected at least ${numSuccessfulQueries * 3 + 1} blocks in result file" +
            s"but got ${segments.size}. " +
            s"Try regenerate the result files.")
        numSuccessfulQueries
      }
      val successfulRuns = Seq.tabulate(numSuccessfulQueries) { i =>
        QueryOutput(
          sql = segments(i * 3 + 1).trim,
          schema = segments(i * 3 + 2).trim,
          output = segments(i * 3 + 3).replaceAll("\\s+$", "")
        )
      }

      // Successful queries, followed by the last known unsuccessful query (CRASH), followed by
      // remaining queries (SKIPPED)
      val crashed = queries.slice(lastKnownSuccessfulQuery + 1, lastKnownSuccessfulQuery + 2)
          .map(QueryOutput(_, "", "CRASH"))
      assert(crashed.isEmpty || crashed.size == 1)
      val skipped = queries.slice(lastKnownSuccessfulQuery + 2, queries.size)
          .map(QueryOutput(_, "", "SKIPPED"))
      assert(successfulRuns.size + crashed.size + skipped.size == queries.size)
      successfulRuns ++ crashed ++ skipped
    }
    (queryOutputs, log4jFilePath)
  }
}

object SparkSubmitQueryRunner {
  val FILEPATH_KEY = "##FILEPATH_KEY##"
  val LOGLEVEL_KEY = "##LOGLEVEL##"
  val LOG4J_FILE =
    s"""
      |# Set everything to be logged to the console
      |log4j.rootLogger=INFO, FA
      |log4j.appender.FA=org.apache.log4j.FileAppender
      |log4j.appender.FA.file=$FILEPATH_KEY
      |log4j.appender.FA.layout=org.apache.log4j.PatternLayout
      |log4j.appender.FA.layout.ConversionPattern=%d{HH:mm:ss.SSS} %t %p %c{1}: %m%n
      |log4j.appender.FA.append=false
      |log4j.appender.FA.Threshold=$LOGLEVEL_KEY
      |
      |# Settings to quiet third party logs that are too verbose
      |log4j.logger.org.sparkproject.jetty=WARN
      |log4j.logger.org.sparkproject.jetty.util.component.AbstractLifeCycle=ERROR
      |log4j.logger.org.apache.spark.repl.SparkIMain$$exprTyper=INFO
      |log4j.logger.org.apache.spark.repl.SparkILoop$$SparkILoopInterpreter=INFO
      |log4j.logger.org.apache.parquet=ERROR
      |log4j.logger.parquet=ERROR
      |
      |# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in
      |SparkSQL with Hive support
      |log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
      |log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
      |""".stripMargin

  def getLog4JFileString(logLevel: String, filePath: Path): String = {
    LOG4J_FILE
      .replace(FILEPATH_KEY, filePath.toString.stripPrefix("file:"))
      .replace(LOGLEVEL_KEY, logLevel)
  }
}
