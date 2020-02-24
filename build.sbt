name := "spark-random-query-generator"

version := "0.0.1-SNAPSHOT"

organization := "org.apache"

scalaVersion := "2.12.10"

val runDataGenerator = inputKey[Unit]("runs DataGernator")

runDataGenerator := {
  import complete.DefaultParsers._
  val args = spaceDelimited("[args]").parsed
  val scalaRun = (runner in run).value
  val classpath = (fullClasspath in Compile).value
  scalaRun.run("org.apache.spark.rqg.comparison.DataGenerator", classpath.map(_.data), args,
    streams.value.log)
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-hive" % "3.0.0-preview2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.4",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "com.typesafe" % "config" % "1.4.0",
  "com.github.scopt" %% "scopt" % "4.0.0-RC2"
)
