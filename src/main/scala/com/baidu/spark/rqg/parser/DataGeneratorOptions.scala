package com.baidu.spark.rqg.parser

import com.baidu.spark.rqg.parser.DataGeneratorOptions.DataSources
import com.baidu.spark.rqg.parser.LoggingOptions.LogLevel
import scopt.OParser

/**
 * Options for the DataGenerator Application
 */
case class DataGeneratorOptions(
    // Logging Options
    logLevel: LogLevel.Value = LoggingOptions.Defaults.logLevel,
    // Connection Options
    dbName: String = ConnectionOptions.Defaults.dbName,
    refHost: String = ConnectionOptions.Defaults.refHost,
    refPort: Int = ConnectionOptions.Defaults.refPort,
    testHost: String = ConnectionOptions.Defaults.testHost,
    testPort: Int = ConnectionOptions.Defaults.testPort,
    // Database Population Options
    randomizationSeed: Int = DataGeneratorOptions.Defaults.randomizationSeed,
    tableCount: Int = DataGeneratorOptions.Defaults.tableCount,
    minColumnCount: Int = DataGeneratorOptions.Defaults.minColumnCount,
    maxColumnCount: Int = DataGeneratorOptions.Defaults.maxColumnCount,
    minRowCount: Int = DataGeneratorOptions.Defaults.minRowCount,
    maxRowCount: Int = DataGeneratorOptions.Defaults.maxRowCount,
    dataSources: Seq[DataSources.Value] = DataGeneratorOptions.Defaults.dataSources)
  extends LoggingOptions[DataGeneratorOptions]
  with ConnectionOptions[DataGeneratorOptions] {

  def validateOptions(): Unit = {
    assert(tableCount > 0, "tableCount must be positive")
  }

  override def withLogLevel(logLevel: LoggingOptions.LogLevel.Value): DataGeneratorOptions =
    copy(logLevel = logLevel)

  override def withRefHost(refHost: String): DataGeneratorOptions = copy(refHost = refHost)

  override def withRefPort(refPort: Int): DataGeneratorOptions = copy(refPort = refPort)

  override def withTestHost(testHost: String): DataGeneratorOptions = copy(testHost = testHost)

  override def withTestPort(testPort: Int): DataGeneratorOptions = copy(testPort = testPort)

  override def withDBName(dbName: String): DataGeneratorOptions = copy(dbName = dbName)
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

        LoggingOptions.loggingParser,

        ConnectionOptions.connectionParser,

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
