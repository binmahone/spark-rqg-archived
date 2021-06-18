package org.apache.spark.rqg.runner

import org.apache.hadoop.fs.Path
import org.apache.spark.rqg.runner.RQGQueryRunnerApp.QueryOutput

trait SparkQueryRunner {
  /**
   * Runs queries.
   * @param queries queries to run
   * @param path path where the output directory for logs from the run will be created.
   * @param pathSuffix suffix to add to the created output directory.
   * @param extraSparkConf any extra Spark configs to set when launching Spark.
   * @return the query outputs and a path pointing to the log file produced by the run.
   */
  def runQueries(
    queries: Seq[String],
    path: Path,
    pathSuffix: String,
    extraSparkConf: Map[String, String] = Map.empty): (Seq[QueryOutput], Path)
}
