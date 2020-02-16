package com.baidu.spark.rqg.parser

import com.baidu.spark.rqg.parser.LoggingOptions.LogLevel
import scopt.OParser

case class DiscrepancySearcherOptions(
    // Logging Options
    logLevel: LogLevel.Value = LoggingOptions.Defaults.logLevel,
    // Connection Options
    dbName: String = ConnectionOptions.Defaults.dbName,
    refHost: String = ConnectionOptions.Defaults.refHost,
    refPort: Int = ConnectionOptions.Defaults.refPort,
    testHost: String = ConnectionOptions.Defaults.testHost,
    testPort: Int = ConnectionOptions.Defaults.testPort,
    // Discrepancy Searcher Options
    randomizationSeed: Int = DiscrepancySearcherOptions.Defaults.randomizationSeed,
    stopOnMismatch: Boolean = DiscrepancySearcherOptions.Defaults.stopOnMismatch,
    stopOnCrash: Boolean = DiscrepancySearcherOptions.Defaults.stopOnCrash,
    queryCount: Int = DiscrepancySearcherOptions.Defaults.queryCount,
    configFile: String = DiscrepancySearcherOptions.Defaults.configFile)
  extends LoggingOptions[DiscrepancySearcherOptions]
  with ConnectionOptions[DiscrepancySearcherOptions] {

  def validateOptions(): Unit = {
    assert(queryCount > 0, "queryCount must be positive")
  }

  override def withLogLevel(logLevel: LoggingOptions.LogLevel.Value): DiscrepancySearcherOptions =
    copy(logLevel = logLevel)

  override def withRefHost(refHost: String): DiscrepancySearcherOptions =
    copy(refHost = refHost)

  override def withRefPort(refPort: Int): DiscrepancySearcherOptions =
    copy(refPort = refPort)

  override def withTestHost(testHost: String): DiscrepancySearcherOptions =
    copy(testHost = testHost)

  override def withTestPort(testPort: Int): DiscrepancySearcherOptions =
    copy(testPort = testPort)

  override def withDBName(dbName: String): DiscrepancySearcherOptions =
    copy(dbName = dbName)
}

object DiscrepancySearcherOptions {
  object Defaults {
    val randomizationSeed = 0
    val stopOnMismatch = false
    val stopOnCrash = false
    val queryCount = 100
    val configFile = "conf/rqg-defaults.json"
  }

  def parse(args: Array[String]): DiscrepancySearcherOptions = {

    val parser: OParser[_, DiscrepancySearcherOptions] = {
      val builder = OParser.builder[DiscrepancySearcherOptions]
      import builder._
      OParser.sequence(
        programName("DiscrepancySearcher"),

        LoggingOptions.loggingParser,

        ConnectionOptions.connectionParser,

        note("Discrepancy Searcher Options"),

        opt[Int]("randomizationSeed")
          .action((i, c) => c.copy(randomizationSeed = i))
          .text("the randomization will be initialized with this seed. Using the same seed " +
            "will produce the same results across runs."),

        opt[Int]("queryCount")
          .action((i, c) => c.copy(queryCount = i))
          .text("The number of queries to generate."),

        opt[String]("configFile")
          .action((s, c) => c.copy(configFile = s))
          .text("Path to a configuration file with all tested systems."),

        note("\nOther Options"),

        help("help").text("prints this usage text")
      )
    }

    OParser.parse(parser, args, DiscrepancySearcherOptions()) match {
      case Some(options) =>
        options.validateOptions()
        options
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}
