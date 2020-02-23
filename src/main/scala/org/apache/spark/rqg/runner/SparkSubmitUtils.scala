package org.apache.spark.rqg.runner

import java.io.{ByteArrayOutputStream, File, FileInputStream, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.Date

import scala.collection.JavaConverters._
import scala.sys.process._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, WildcardFileFilter}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SecurityManager, SparkConf, TestUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.processStreamByLine

object SparkSubmitUtils extends Logging {

  def tryDownloadSpark(version: String, path: String): Unit = {
    // Try a few mirrors first; fall back to Apache archive
    val mirrors =
      (0 until 2).flatMap { _ =>
        try {
          Some(getStringFromUrl("https://www.apache.org/dyn/closer.lua?preferred=true"))
        } catch {
          // If we can't get a mirror URL, skip it. No retry.
          case _: Exception => None
        }
      }
    val sites = mirrors.distinct :+ "https://archive.apache.org/dist"
    logInfo(s"Trying to download Spark $version from $sites")
    for (site <- sites) {
      val filename = s"spark-$version-bin-hadoop2.7.tgz"
      val url = s"$site/spark/spark-$version/$filename"
      logInfo(s"Downloading Spark $version from $url")
      try {
        getFileFromUrl(url, path, filename)
        val downloaded = new File(path, filename).getCanonicalPath
        val targetDir = new File(path, s"spark-$version").getCanonicalPath

        Seq("mkdir", targetDir).!
        val exitCode = Seq("tar", "-xzf", downloaded, "-C", targetDir, "--strip-components=1").!
        Seq("rm", downloaded).!

        // For a corrupted file, `tar` returns non-zero values. However, we also need to check
        // the extracted file because `tar` returns 0 for empty file.
        val sparkSubmit = new File(path, s"spark-$version/bin/spark-submit")
        if (exitCode == 0 && sparkSubmit.exists()) {
          return
        } else {
          Seq("rm", "-rf", targetDir).!
        }
      } catch {
        case ex: Exception =>
          logWarning(s"Failed to download Spark $version from $url: ${ex.getMessage}")
      }
    }
    sys.error(s"Unable to download Spark $version")
  }

  def getFileFromUrl(urlString: String, targetDir: String, filename: String): Unit = {
    val conf = new SparkConf
    // if the caller passes the name of an existing file, we want doFetchFile to write over it with
    // the contents from the specified url.
    conf.set("spark.files.overwrite", "true")
    val securityManager = new SecurityManager(conf)
    val hadoopConf = new Configuration

    val outDir = new File(targetDir)
    if (!outDir.exists()) {
      outDir.mkdirs()
    }

    // propagate exceptions up to the caller of getFileFromUrl
    Utils.doFetchFile(urlString, outDir, filename, conf, securityManager, hadoopConf)
  }

  private def getStringFromUrl(urlString: String): String = {
    val contentFile = File.createTempFile("string-", ".txt")
    contentFile.deleteOnExit()

    // exceptions will propagate to the caller of getStringFromUrl
    getFileFromUrl(urlString, contentFile.getParent, contentFile.getName)

    val contentPath = Paths.get(contentFile.toURI)
    new String(Files.readAllBytes(contentPath), StandardCharsets.UTF_8)
  }

  def runSparkSubmit(
      args: Seq[String],
      sparkHome: String,
      timeoutInSeconds: Int = 0,
      verbose: Boolean = false): Unit = {
    val sparkSubmitFile = if (Utils.isWindows) {
      new File(s"$sparkHome\\bin\\spark-submit.cmd")
    } else {
      new File(s"$sparkHome/bin/spark-submit")
    }
    val process = Utils.executeCommand(
      Seq(sparkSubmitFile.getCanonicalPath) ++ args,
      new File(sparkHome),
      Map("SPARK_HOME" -> sparkHome))

    def appendToOutput(s: String): Unit = {
      val logLine = s"${new Timestamp(new Date().getTime)} - $s"
      // TODO: should we use verbose to control the output
      // scalastyle:off println
      println(logLine)
      // scalastyle:on println
    }
    val stdoutThread = processStreamByLine(
      s"read stdout for ${sparkSubmitFile.getName}", process.getInputStream, appendToOutput)

    try {
      val exitCode = if (timeoutInSeconds <= 0) {
        process.waitFor()
      } else {
        val now = System.currentTimeMillis()
        val timeoutInMillis = 1000L * timeoutInSeconds
        val finish = now + timeoutInMillis
        while (process.isAlive && (System.currentTimeMillis() < finish )) {
          Thread.sleep(10)
        }
        if (process.isAlive) {
          throw new InterruptedException(
            s"spark-submit interrupted due to timeout, See the log4j logs for more detail.")
        }
        process.exitValue()
      }
      if (exitCode != 0) {
        // include logs in output. Note that logging is async and may not have completed
        // at the time this exception is raised
        stdoutThread.join()
        sys.error(
          s"spark-submit returned with exit code $exitCode, See the log4j logs for more detail.")
      }
    } finally {
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
  }

  def createSparkAppJar(clazz: Class[_]): File = {
    val tempDir = Utils.createTempDir()
    val classPath = new File(clazz.getProtectionDomain.getCodeSource.getLocation.getPath)
    val jarFile = if (classPath.getName.endsWith(".jar")) {
      // Program is running from a jar package, use it directly
      classPath
    } else {
      // Program is running with a directory classpath, package target class to a jar
      val className = clazz.getSimpleName.stripSuffix("$")
      val prefixDir = clazz.getPackage.getName.replace(".", "/")
      val files = FileUtils.listFiles(
        classPath,
        new WildcardFileFilter(s"*$className*"),
        DirectoryFileFilter.INSTANCE)
      val jarFile = new File(tempDir, "sparkAppJar-%s.jar".format(System.currentTimeMillis()))
      TestUtils.createJar(files.asScala.toSeq, jarFile, Some(prefixDir))
      jarFile
    }
    jarFile
  }

  def stringToFile(str: String): File = {
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir, "stringFile-%s.txt".format(System.currentTimeMillis()))
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }
}
