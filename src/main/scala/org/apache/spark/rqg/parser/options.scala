package org.apache.spark.rqg.parser

import scopt.OParser

/**
 * Options for logging
 */
trait LoggingOptions[R] {
  import LoggingOptions._
  def withLogLevel(logLevel: LogLevel.Value): R
}

object LoggingOptions {
  def loggingParser[R <: LoggingOptions[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
      note("Logging Options"),
      opt[LogLevel.Value]("logLevel")
        .action((v, c) => c.withLogLevel(logLevel = v))
        .text("The log level to use. choices: DEBUG, INFO, WARN, ERROR"),
      note("")
    )
  }

  object Defaults {
    val logLevel: LogLevel.Value = LogLevel.INFO
  }

  object LogLevel extends Enumeration {
    type LogLevel = Value
    val DEBUG, INFO, WARN, ERROR = Value
  }

  implicit val logLevelRead: scopt.Read[LogLevel.Value] = scopt.Read.reads(LogLevel withName)
}

/**
 * Options for Database
 */
trait DatabaseOptions[R] {
  def withDatabaseName(dbName: String): R
}

object DatabaseOptions {
  def databaseParser[R <: DatabaseOptions[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]
    import builder._
    OParser.sequence(
      note("Database Options"),

      opt[String]("dbName")
        .action((s, c) => c.withDatabaseName(dbName = s))
        .text("The name of the database to use. Ex: default"),

      note("")
    )
  }

  object Defaults {
    val dbName: String = "default"
  }
}

/**
 * Options for JDBC Connection
 */
trait ConnectionOptions[R] {
  def withDBName(dbName: String): R
  def withRefHost(refHost: String): R
  def withRefPort(refPort: Int): R
  def withTestHost(testHost: String): R
  def withTestPort(testPort: Int): R
}


object ConnectionOptions {
  def connectionParser[R <: ConnectionOptions[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]

    import builder._

    OParser.sequence(
      note("Database Name Options"),

      opt[String]("dbName")
        .action((s, c) => c.withDBName(dbName = s))
        .text("The name of the database to use. Ex: default"),

      note("\nReference System Options"),

      opt[String]("refHost")
        .action((s, c) => c.withRefHost(refHost = s))
        .text("The hostname where the reference system is running."),

      opt[Int]("refPort")
        .action((i, c) => c.withRefPort(refPort = i))
        .text("The port on which the reference system is running."),

      note("\nTest System Options"),

      opt[String]("testHost")
        .action((s, c) => c.withTestHost(testHost = s))
        .text("The hostname where the reference system is running."),

      opt[Int]("testPort")
        .action((i, c) => c.withTestPort(testPort = i))
        .text("The port on which the reference system is running."),

      note("")
    )
  }

  object Defaults {
    val dbName: String = "default"
    val refHost: String = "localhost"
    val refPort: Int = 10000
    val testHost: String = "localhost"
    val testPort: Int = 10000
  }
}

/**
 * Options for Database
 */
trait SparkSubmitOptions[R] {
  def withTimeout(timeout: Int): R
  def withRefVersion(refSparkVersion: String): R
  def withRefSparkHome(refSparkHome: String): R
  def withRefMaster(master: String): R
  def withTestVersion(testSparkVersion: String): R
  def withTestSparkHome(testSparkHome: String): R
  def withTestMaster(master: String): R
}

object SparkSubmitOptions {
  def sparkSubmitParser[R <: SparkSubmitOptions[R]]: OParser[_, R] = {
    val builder = OParser.builder[R]

    import builder._

    OParser.sequence(
      note("Spark Submit Options"),

      opt[Int]("timeout")
        .action((i, c) => c.withTimeout(i))
        .text("timeout in seconds for running a SparkSubmit App"),

      note("\nReference System Options"),

      opt[String]("refSparkVersion")
        .action((s, c) => c.withRefVersion(refSparkVersion = s))
        .text("The spark version of the reference system"),

      opt[String]("refSparkHome")
        .action((s, c) => c.withRefSparkHome(refSparkHome = s))
        .text("The spark home dir on which the reference system is running. " +
          "the program will try to download a spark release if this is not provided"),

      opt[String]("refMaster")
        .action((s, c) => c.withRefSparkHome(refSparkHome = s))
        .text("The master option to submit a spark app, e.g. local[*], yarn, spark://IP:PORT"),

      note("\nTest System Options"),

      opt[String]("testSparkVersion")
        .action((s, c) => c.withTestVersion(testSparkVersion = s))
        .text("The spark version of the test system"),

      opt[String]("testSparkHome")
        .action((s, c) => c.withTestSparkHome(testSparkHome = s))
        .text("The spark home dir on which the test system is running. " +
          "the program will try to download a spark release if this is not provided"),

      opt[String]("testMaster")
        .action((s, c) => c.withRefSparkHome(refSparkHome = s))
        .text("The master option to submit a spark app, e.g. local[*], yarn, spark://IP: PORT"),

      note("")
    )
  }

  object Defaults {
    val timeout: Int = 0
    val refSparVersion: String = "2.4.5"
    val refSparkHome: Option[String] = None
    val refMaster: String = "local[*]"
    val testSparkVersion: String = "3.0.0-preview2"
    val testSparkHome: Option[String] = None
    val testMaster: String = "local[*]"
  }
}
