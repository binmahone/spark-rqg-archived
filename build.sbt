name := "spark-random-query-generator"

// logLevel := Level.Debug

version := "0.0.1-SNAPSHOT"

organization := "org.apache"

scalaVersion := "2.12.10"

enablePlugins(PackPlugin)

packMain := Map(
  "data-generator" -> "org.apache.spark.rqg.comparison.DataGenerator",
  "query-generator" -> "org.apache.spark.rqg.comparison.QueryGenerator")

packExtraClasspath := Map(
  "data-generator" -> Seq("${PROG_HOME}/conf"),
  "query-generator" -> Seq("${PROG_HOME}/conf"))

packResourceDir += (baseDirectory.value / "conf" -> "conf")

packGenerateMakefile := false

val runQueryGenerator = inputKey[Unit]("runs QueryGenerator")

runQueryGenerator := {
  import complete.DefaultParsers._
  val args = spaceDelimited("[args]").parsed
  val scalaRun = (runner in run).value
  val classpath = (fullClasspath in Compile).value
  scalaRun.run("org.apache.spark.rqg.comparison.QueryGenerator",
    classpath.map(_.data) ++ ((baseDirectory.value / "conf") ** "*").get, args,
    streams.value.log)
}

val runDataGenerator = inputKey[Unit]("runs DataGenerator")

runDataGenerator := {
  import complete.DefaultParsers._
  val args = spaceDelimited("[args]").parsed
  val scalaRun = (runner in run).value
  val classpath = (fullClasspath in Compile).value
  scalaRun.run("org.apache.spark.rqg.comparison.DataGenerator",
    classpath.map(_.data) ++ ((baseDirectory.value / "conf") ** "*").get, args,
    streams.value.log)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-hive" % "3.1.1",
  "org.apache.hadoop" % "hadoop-common" % "2.7.4",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "com.typesafe" % "config" % "1.4.0",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2",
  "org.hibernate" % "hibernate-core" % "3.6.8.Final"
)
