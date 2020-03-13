package org.apache.spark.rqg.runner

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.rqg.runner.RQGQueryRunnerApp.QueryOutput

class SparkSubmitQueryRunner(
    version: String,
    sparkHomeOpt: Option[String] = None,
    master: String = "local[*]",
    timeout: Int = 0)
  extends SparkQueryRunner with Logging {

  private val sparkTestingDir = new File("/tmp/test-spark")

  private val outputDir =
    new Path(
      new Path(FileSystem.get(new Configuration()).getHomeDirectory, s"rqg_data"),
      s"${version}_output").toString

  override def runQueries(
      queries: Seq[String], extraSparkConf: Map[String, String] = Map.empty): Seq[QueryOutput] = {

    val sparkHome = sparkHomeOpt.map(new File(_))
      .getOrElse(new File(sparkTestingDir, s"spark-$version"))

    if (!sparkHome.exists()) {
      SparkSubmitUtils.tryDownloadSpark(version, sparkTestingDir.getCanonicalPath)
    }

    val jarFile = SparkSubmitUtils.createSparkAppJar(RQGQueryRunnerApp.getClass)

    val queryFile = SparkSubmitUtils.stringToFile(queries.mkString("\n"))

    val resultFile = new Path(outputDir, "resultFile-%s.txt".format(System.currentTimeMillis()))

    val sparkArgs = Seq(
      "--class", RQGQueryRunnerApp.getClass.getName.stripSuffix("$"),
      "--name", s"Spark $version RQG Runner",
      "--master", master,
      "--files", queryFile.toString)
    val configArgs = extraSparkConf.flatMap(e => Seq("--conf", e._1 + "=" + e._2))
    val appArgs = Seq(
      jarFile.toString,
      queryFile.getName,
      resultFile.toString)

    val args = sparkArgs ++ configArgs ++ appArgs
    SparkSubmitUtils.runSparkSubmit(args, sparkHome.getCanonicalPath, timeout)

    val is = resultFile.getFileSystem(new Configuration()).open(resultFile)

    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        is.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    } finally {
      is.close()
    }

    val outputString = new String(outStream.toByteArray)

    val queryOutputs: Seq[QueryOutput] = {
      val segments = outputString.split("-- !query.+\n")

      // each query has 3 segments, plus the header
      assert(segments.size == queries.size * 3 + 1,
        s"Expected ${queries.size * 3 + 1} blocks in result file but got ${segments.size}. " +
          s"Try regenerate the result files.")
      Seq.tabulate(queries.size) { i =>
        QueryOutput(
          sql = segments(i * 3 + 1).trim,
          schema = segments(i * 3 + 2).trim,
          output = segments(i * 3 + 3).replaceAll("\\s+$", "")
        )
      }
    }
    queryOutputs
  }
}
