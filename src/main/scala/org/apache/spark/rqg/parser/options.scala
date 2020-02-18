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
