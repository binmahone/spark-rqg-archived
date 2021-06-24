package org.apache.spark.rqg.parser

import scopt.OParser

case class QueryGeneratorOptions(
    // Database Options
    dbName: String = DatabaseOptions.Defaults.dbName,
    // Spark Runner Options
    timeout: Int = SparkSubmitOptions.Defaults.timeout,
    refSparkVersion: String = SparkSubmitOptions.Defaults.refSparkVersion,
    refSparkHome: Option[String] = SparkSubmitOptions.Defaults.refSparkHome,
    refMaster: String = SparkSubmitOptions.Defaults.refMaster,
    testSparkVersion: String = SparkSubmitOptions.Defaults.testSparkVersion,
    testSparkHome: Option[String] = SparkSubmitOptions.Defaults.testSparkHome,
    testMaster: String = SparkSubmitOptions.Defaults.refMaster,
    verbose: Boolean = SparkSubmitOptions.Defaults.verbose,
    // Discrepancy Searcher Options
    randomizationSeed: Int = QueryGeneratorOptions.Defaults.randomizationSeed,
    stopOnMismatch: Boolean = QueryGeneratorOptions.Defaults.stopOnMismatch,
    stopOnCrash: Boolean = QueryGeneratorOptions.Defaults.stopOnCrash,
    skipDbSetup: Boolean = QueryGeneratorOptions.Defaults.skipDbSetup,
    useParquet: Boolean = QueryGeneratorOptions.Defaults.useParquet,
    queryCount: Int = QueryGeneratorOptions.Defaults.queryCount,
    batchSize: Int = QueryGeneratorOptions.Defaults.batchSize,
    configFile: String = QueryGeneratorOptions.Defaults.configFile,
    generateViewsCount: Int = QueryGeneratorOptions.Defaults.generateViewsCount,
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

  override def withVerbose(verbose: Boolean): QueryGeneratorOptions =
    copy(verbose = verbose)
}

object QueryGeneratorOptions {

  def parse(args: Array[String]): QueryGeneratorOptions = {
    val parser: OParser[_, QueryGeneratorOptions] = {
      val builder = OParser.builder[QueryGeneratorOptions]
      import builder._
      OParser.sequence(
        programName("QueryGenerator"),
        DatabaseOptions.databaseParser,
        SparkSubmitOptions.sparkSubmitParser,
        note("Query Generator Options"),
        opt[Boolean]("dryRun")
          .action((i, c) => c.copy(dryRun = i))
          .text("Generate and print queries, but don't execute them."),
        opt[Boolean]("stopOnCrash")
          .action((i, c) => c.copy(stopOnCrash = i))
          .text("Quit the RQG if Spark crashes or any query throws an exception."),
        opt[Boolean]("stopOnMismatch")
          .action((i, c) => c.copy(stopOnMismatch = i))
          .text("Quit the RQG if a mismatch between the reference and test is found."),
        opt[Boolean]("skipDbSetup")
          .action((i, c) => c.copy(skipDbSetup = i))
          .text("Skips setting up the database, assuming it was done already. Useful for repeated" +
            "runs of the RQG."),
        opt[Boolean]("useParquet")
            .action((i, c) => c.copy(useParquet = i))
            .text("Force Parquet as the data source when loading tables."),
        opt[Int]("randomizationSeed")
          .action((i, c) => c.copy(randomizationSeed = i))
          .text(
            "the randomization will be initialized with this seed. Using the same seed " +
              "will produce the same results across runs."
          ),
        opt[Int]("queryCount")
          .action((i, c) => c.copy(queryCount = i))
          .text("The number of queries to generate."),
        opt[Int]("batchSize")
          .action((i, c) => c.copy(batchSize = i))
          .text("Number of queries to generate and run in a single call to spark-submit."),
        opt[String]("configFile")
          .action((s, c) => c.copy(configFile = s))
          .text("Path to a configuration file with all tested systems."),
        opt[Int]("generateViewsCount")
          .action((i, c) => c.copy(generateViewsCount = i))
          .text("Number of temporary views to generate."),
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
    val skipDbSetup = false
    val queryCount = 100
    val batchSize = 20
    val configFile = ""
    val dryRun = false
    val useParquet = false
    val generateViewsCount = 10
  }

}
