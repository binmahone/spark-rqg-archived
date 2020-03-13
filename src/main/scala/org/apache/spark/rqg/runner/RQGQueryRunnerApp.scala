package org.apache.spark.rqg.runner

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.io.Source
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkFiles}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.IntervalUtils.{toIso8601String, toMultiUnitsString, toSqlStandardString}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.IntervalStyle.{ISO_8601, MULTI_UNITS, SQL_STANDARD}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Most code is copied from SQLQueryTestSuite
 */
object RQGQueryRunnerApp {

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
   * This method handles exceptions occurred during query execution as they may need special care
   * to become comparable to the expected output.
   *
   * @param result a function that returns a pair of schema and output
   */
  protected def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
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
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  private def getNormalizedResult(session: SparkSession, sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeTableCommand | _: DescribeColumnCommand => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = session.sql(sql)
    val schema = df.schema.catalogString
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

    val hadoopConf = new Configuration()
    val outputFile = new Path(args(1))
    val fs = outputFile.getFileSystem(hadoopConf)
    val parent = outputFile.getParent
    if (fs.exists(parent)) {
      assert(fs.mkdirs(parent), "Could not create directory: " + parent)
    }

    val outputs = Source.fromFile(inputFile).getLines().toSeq.map { sql =>
      val (schema, output) = handleExceptions(getNormalizedResult(sparkSession, sql))
      // We might need to do some query canonicalization in the future.
      QueryOutput(
        sql = sql,
        schema = schema,
        output = output.mkString("\n").replaceAll("\\s+$", ""))
    }

    val goldenOutput = {
      s"-- Automatically generated by ${getClass.getSimpleName}\n" +
        s"-- Number of queries: ${outputs.size}\n\n\n" +
        outputs.zipWithIndex.map{case (qr, i) => qr.toString(i)}.mkString("\n\n\n") + "\n"
    }

    val os = fs.create(outputFile)
    os.write(goldenOutput.getBytes())
    os.close()

    sparkSession.stop()
  }

  object HiveResult {
    /**
     * Returns the result as a hive compatible sequence of strings. This is used in tests and
     * `SparkSQLDriver` for CLI applications.
     */
    def hiveResultString(executedPlan: SparkPlan): Seq[String] = executedPlan match {
      case ExecutedCommandExec(_: DescribeTableCommand) =>
        // If it is a describe command for a Hive table, we want to have the output format
        // be similar with Hive.
        executedPlan.executeCollectPublic().map {
          case Row(name: String, dataType: String, comment) =>
            Seq(name, dataType,
              Option(comment.asInstanceOf[String]).getOrElse(""))
              .map(s => String.format(s"%-20s", s))
              .mkString("\t")
        }
      // SHOW TABLES in Hive only output table names, while ours output database, table name, isTemp.
      case command @ ExecutedCommandExec(s: ShowTablesCommand) if !s.isExtended =>
        command.executeCollect().map(_.getString(1))
      case other =>
        val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
        // We need the types so we can output struct field names
        val types = executedPlan.output.map(_.dataType)
        // Reformat to match hive tab delimited output.
        result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t"))
    }

    private val primitiveTypes = Seq(
      StringType,
      IntegerType,
      LongType,
      DoubleType,
      FloatType,
      BooleanType,
      ByteType,
      ShortType,
      DateType,
      TimestampType,
      BinaryType)

    private lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
    private lazy val dateFormatter = DateFormatter(zoneId)
    private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    private def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (decimal, DecimalType()) => decimal.toString
      case (interval: CalendarInterval, CalendarIntervalType) =>
        SQLConf.get.intervalOutputStyle match {
          case SQL_STANDARD => toSqlStandardString(interval)
          case ISO_8601 => toIso8601String(interval)
          case MULTI_UNITS => toMultiUnitsString(interval)
        }
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    /** Formats a datum (based on the given data type) and returns the string representation. */
    def toHiveString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (d: Date, DateType) => dateFormatter.format(DateTimeUtils.fromJavaDate(d))
      case (t: Timestamp, TimestampType) =>
        DateTimeUtils.timestampToString(timestampFormatter, DateTimeUtils.fromJavaTimestamp(t))
      case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
      case (decimal: java.math.BigDecimal, DecimalType()) => decimal.toPlainString
      case (interval: CalendarInterval, CalendarIntervalType) =>
        SQLConf.get.intervalOutputStyle match {
          case SQL_STANDARD => toSqlStandardString(interval)
          case ISO_8601 => toIso8601String(interval)
          case MULTI_UNITS => toMultiUnitsString(interval)
        }
      case (interval, CalendarIntervalType) => interval.toString
      case (other, _ : UserDefinedType[_]) => other.toString
      case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
    }
  }
}
