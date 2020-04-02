package org.apache.spark.rqg.parser

import scopt.OParser

case class QueryGeneratorOptions(
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
    // Discrepancy Searcher Options
    randomizationSeed: Int = QueryGeneratorOptions.Defaults.randomizationSeed,
    stopOnMismatch: Boolean = QueryGeneratorOptions.Defaults.stopOnMismatch,
    stopOnCrash: Boolean = QueryGeneratorOptions.Defaults.stopOnCrash,
    queryCount: Int = QueryGeneratorOptions.Defaults.queryCount,
    configFile: String = QueryGeneratorOptions.Defaults.configFile,
    // Run mode
    dryRun: Boolean = QueryGeneratorOptions.Defaults.dryRun)
  extends DatabaseOptions[QueryGeneratorOptions]
  with SparkSubmitOptions[QueryGeneratorOptions] {

  override def withDatabaseName(dbName: String): QueryGeneratorOptions =
    copy(dbName = dbName)

  override def withTimeout(timeout: Int): QueryGeneratorOptions =
    copy(timeout = timeout)

  override def withRefVersion(refVersion: String): QueryGeneratorOptions =
    copy(refSparkVersion = refVersion)

  override def withRefSparkHome(refSparkHome: String): QueryGeneratorOptions =
    copy(refSparkHome = Some(refSparkHome))

  override def withRefMaster(master: String): QueryGeneratorOptions =
    copy(refMaster = master)

  override def withTestVersion(testVersion: String): QueryGeneratorOptions =
    copy(testSparkVersion = testVersion)

  override def withTestSparkHome(testSparkHome: String): QueryGeneratorOptions =
    copy(testSparkHome = Some(testSparkHome))

  override def withTestMaster(master: String): QueryGeneratorOptions =
    copy(testMaster = master)
}

object QueryGeneratorOptions {

  def parse(args: Array[String]): QueryGeneratorOptions = {

    val parser: OParser[_, QueryGeneratorOptions] = {
      val builder = OParser.builder[QueryGeneratorOptions]
      import builder._
      OParser.sequence(
        programName("DataGenerator"),
        DatabaseOptions.databaseParser,
        SparkSubmitOptions.sparkSubmitParser,
        note("Query Generator Options"),
        opt[Boolean]("dryRun")
          .action((i, c) => c.copy(dryRun = i))
          .text("dry run will only display but does not excecute query"),
        opt[Int]("randomizationSeed")
          .action((i, c) => c.copy(randomizationSeed = i))
          .text(
            "the randomization will be initialized with this seed. Using the same seed " +
              "will produce the same results across runs."
          ),
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

    OParser.parse(parser, args, QueryGeneratorOptions()) match {
      case Some(options) =>
        options
      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }

  object Defaults {
    val randomizationSeed = 0
    val stopOnMismatch = false
    val stopOnCrash = false
    val queryCount = 100
    val configFile = ""
    val dryRun = false
  }

}
