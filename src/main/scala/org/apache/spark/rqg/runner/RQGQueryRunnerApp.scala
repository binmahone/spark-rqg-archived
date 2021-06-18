package org.apache.spark.rqg.runner

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.io.Source
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkException, SparkFiles}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.{HiveResult, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Runs queries as a Spark application. The entry point into this class is generally `spark-submit`.
 * Most of the code is copied from SQLQueryTestSuite.
 *
 * The main() function expects three arguments:
 * - The filename of the file holding the SQL queries, one on each line
 * - Path of the result file, where results should be flushed by this app.
 * - A string 'true' or 'false' that indicates whether to print verbose or quiet output. Verbose
 * output will print queries as they are being executed.
 */
object RQGQueryRunnerApp extends Logging {
  /**
   * Holds query output.
   * @param sql SQL text of the query
   * @param schema Normalized schema of the query.
   * @param output Output of the query: either an exception message or a dataset.
   */
  case class QueryOutput(sql: String, schema: String, output: String) {
    def toString(queryIndex: Int): String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query $queryIndex\n" +
        sql + "\n" +
        s"-- !query $queryIndex schema\n" +
        schema + "\n" +
        s"-- !query $queryIndex output\n" +
        output
    }
  }

  /**
   * Handles exceptions that occurred during query execution as they may need special care
   * to become comparable to the expected output.
   *
   * @param result a function that returns a pair of schema and output
   */
  protected def handleExceptions(
    sparkSession: SparkSession,
    result: => (String, Seq[String])): (String, Seq[String]) = {
    val emptySchema = StructType(Seq.empty).catalogString
    try {
      result
    } catch {
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        val msg = if (a.plan.nonEmpty) a.getSimpleMessage else a.getMessage
        (emptySchema, Seq(a.getClass.getName, msg.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        val cause = s.getCause
        (emptySchema, Seq(cause.getClass.getName, cause.getMessage))
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (emptySchema, Seq(e.getClass.getName, e.getMessage))
      case fatal =>
        val msg = s"Unknown exception ${fatal.getClass.getName} - ${fatal.getMessage}"
        logError(msg)
        sys.error(msg)
    } finally {
      if (sparkSession.sparkContext.isStopped) {
        // If the context stopped, the executor most likely crashed. Exit here
        logError("SparkContext was stopped, most likely due to an executor crash." +
          "See logs to see what went wrong.")
        sys.error("SparkContext stopped")
      }
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  private def getNormalizedResult(
    sparkSession: SparkSession,
    sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeTableCommand | _: DescribeColumnCommand => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = sparkSession.sql(sql)
    val schema = df.schema.catalogString

    logInfo(s"Executing query $sql")
    logInfo(s"Plan: ${df.queryExecution.executedPlan}")

    // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
    val answer = HiveResult.hiveResultString(df.queryExecution.executedPlan).map(replaceNotIncludedMsg)

    // If the output is not pre-sorted, sort it.
    if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)
  }

  protected def replaceNotIncludedMsg(line: String): String = {
    val notIncludedMsg = "[not included in comparison]"
    val clsName = this.getClass.getCanonicalName
    line.replaceAll("#\\d+", "#x")
      .replaceAll(
        s"Location.*$clsName/",
        s"Location $notIncludedMsg/{warehouse_dir}/")
      .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      .replaceAll("\\*\\(\\d+\\) ", "*") // remove the WholeStageCodegen codegenStageIds
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val inputFile = new File(SparkFiles.getRootDirectory(), args(0))
    val outputStream = new PrintWriter(new File(args(1)))
    val printVerbose = args(2).toBoolean

    val queries = Source.fromFile(inputFile).getLines().toSeq
    val goldenFileHeader = {
      s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of attempted queries: ${queries.size}\n\n\n"
    }
    outputStream.write(goldenFileHeader)
    outputStream.flush()

    queries.zipWithIndex.foreach {
      case (sql, queryIdx) =>
        if (printVerbose) {
          println(s"\tQuery $queryIdx running - $sql")
        } else {
          println(s"\tQuery $queryIdx running.")
        }

        // Run the query and collect exceptions.
        val (schema, output) =
          handleExceptions(sparkSession, getNormalizedResult(sparkSession, sql))

        logInfo(s"Query $queryIdx completed")

        // This message is required, since the RQG driver listens for it as a signal of query
        // completion.
        println(s"\tQuery $queryIdx completed.")
        // Flush so listener picks up the query completion event.
        System.out.flush()

        // TODO(shoumik): we might need to do some query canonicalization in the future.
        val queryOutput = QueryOutput(
          sql = sql,
          schema = schema,
          output = output.mkString("\n").replaceAll("\\s+$", ""))
        outputStream.write(queryOutput.toString(queryIdx) + "\n\n\n")
        outputStream.flush()
    }

    outputStream.close()
    sparkSession.stop()
  }
}
