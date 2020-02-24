package org.apache.spark.rqg.parser

import org.apache.spark.rqg.parser.DataGeneratorOptions.DataSources
import scopt.OParser

/**
 * Options for the DataGenerator Application
 */
case class DataGeneratorOptions(
    // Database Options
    dbName: String = DatabaseOptions.Defaults.dbName,
    // Spark Runner Options
    timeout: Int = SparkSubmitOptions.Defaults.timeout,
    refSparkVersion: String = SparkSubmitOptions.Defaults.refSparVersion,
    refSparkHome: Option[String] = SparkSubmitOptions.Defaults.refSparkHome,
    refMaster: String = SparkSubmitOptions.Defaults.refMaster,
    testSparkVersion: String = SparkSubmitOptions.Defaults.testSparkVersion,
    testSparkHome: Option[String] = SparkSubmitOptions.Defaults.testSparkHome,
    testMaster: String = SparkSubmitOptions.Defaults.refMaster,
    // Database Population Options
    randomizationSeed: Int = DataGeneratorOptions.Defaults.randomizationSeed,
    tableCount: Int = DataGeneratorOptions.Defaults.tableCount,
    minColumnCount: Int = DataGeneratorOptions.Defaults.minColumnCount,
    maxColumnCount: Int = DataGeneratorOptions.Defaults.maxColumnCount,
    minRowCount: Int = DataGeneratorOptions.Defaults.minRowCount,
    maxRowCount: Int = DataGeneratorOptions.Defaults.maxRowCount,
    dataSources: Seq[DataSources.Value] = DataGeneratorOptions.Defaults.dataSources)
  extends DatabaseOptions[DataGeneratorOptions]
  with SparkSubmitOptions[DataGeneratorOptions] {

  def validateOptions(): Unit = {
    assert(tableCount > 0, "tableCount must be positive")
  }

  override def withDatabaseName(dbName: String): DataGeneratorOptions =
    copy(dbName = dbName)

  override def withTimeout(timeout: Int): DataGeneratorOptions =
    copy(timeout = timeout)

  override def withRefVersion(refVersion: String): DataGeneratorOptions =
    copy(refSparkVersion = refVersion)

  override def withRefSparkHome(refSparkHome: String): DataGeneratorOptions =
    copy(refSparkHome = Some(refSparkHome))

  override def withRefMaster(master: String): DataGeneratorOptions =
    copy(refMaster = master)

  override def withTestVersion(testVersion: String): DataGeneratorOptions =
    copy(testSparkVersion = testVersion)

  override def withTestSparkHome(testSparkHome: String): DataGeneratorOptions =
    copy(testSparkHome = Some(testSparkHome))

  override def withTestMaster(master: String): DataGeneratorOptions =
    copy(testMaster = master)
}

/**
 * Parse a set of command-line arguments into a [[DataGeneratorOptions]] object
 */
object DataGeneratorOptions {

  object DataSources extends Enumeration {
    type DataSources = Value
    val PARQUET, JSON, HIVE = Value
  }

  implicit val dataSourcesRead: scopt.Read[DataSources.Value] =
    scopt.Read.reads(DataSources withName)

  object Defaults {
    val randomizationSeed = 1
    val dbName = "default"
    val tableCount = 10
    val minColumnCount = 1
    val maxColumnCount = 100
    val minRowCount: Int = math.pow(10, 3).toInt
    val maxRowCount: Int = math.pow(10, 6).toInt
    val dataSources: Seq[DataSources.Value] =
      Seq(DataSources.PARQUET, DataSources.JSON, DataSources.HIVE)
  }

  def parse(args: Array[String]): DataGeneratorOptions = {

    val parser: OParser[_, DataGeneratorOptions] = {
      val builder = OParser.builder[DataGeneratorOptions]
      import builder._
      OParser.sequence(
        programName("DataGenerator"),

        DatabaseOptions.databaseParser,

        SparkSubmitOptions.sparkSubmitParser,

        note("Database Population Options"),

        opt[Int]("randomizationSeed")
          .action((i, c) => c.copy(randomizationSeed = i))
          .text("the randomization will be initialized with this seed. Using the same seed " +
            "will produce the same results across runs."),

        opt[Int]("tableCount")
          .action((i, c) => c.copy(tableCount = i))
          .text("The number of tables to generate."),

        opt[Int]("minColumnCount")
          .action((i, c) => c.copy(minColumnCount = i))
          .text("The minimum number of columns to generate per table."),

        opt[Int]("maxColumnCount")
          .action((i, c) => c.copy(maxColumnCount = i))
          .text("The maximum number of columns to generate per table."),

        opt[Int]("minRowCount")
          .action((i, c) => c.copy(minRowCount = i))
          .text("The minimum number of rows to generate per table."),

        opt[Int]("maxRowCount")
          .action((i, c) => c.copy(maxRowCount = i))
          .text("The maximum number of rows to generate per table."),

        opt[Seq[DataSources.Value]]("dataSources")
          .action((seq, c) => c.copy(dataSources = seq))
          .text("A comma separated list of storage formats to use. choices: PARQUET, JSON, HIVE"),

        note("\nOther Options"),

        help("help").text("prints this usage text")
      )
    }

    OParser.parse(parser, args, DataGeneratorOptions()) match {
      case Some(options) =>
        options.validateOptions()
        options
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}
