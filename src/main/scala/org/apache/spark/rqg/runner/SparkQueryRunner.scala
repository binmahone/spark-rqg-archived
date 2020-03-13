package org.apache.spark.rqg.runner

import org.apache.spark.rqg.runner.RQGQueryRunnerApp.QueryOutput

trait SparkQueryRunner {
  def runQueries(queries: Seq[String], extraSparkConf: Map[String, String] = Map.empty): Seq[QueryOutput]
}
