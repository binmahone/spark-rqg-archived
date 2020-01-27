name := "spark-random-query-generator"

version := "0.0.1-SNAPSHOT"

organization := "com.baidu"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.hadoop" % "hadoop-common" % "2.7.4",
  "org.apache.hive" % "hive-jdbc" % "1.2.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe" % "config" % "1.4.0"
)
