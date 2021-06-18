package org.apache.spark.rqg.runner

import java.io.{File, InputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.sys.process._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, WildcardFileFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SecurityManager, SparkConf, TestUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


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
    logDebug(s"Trying to download Spark $version from $sites")
    for (site <- sites) {
      val filename = s"spark-$version-bin-hadoop2.7.tgz"
      val url = s"$site/spark/spark-$version/$filename"
      logDebug(s"Downloading Spark $version from $url")
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

  /**
   * calls `processLine` on each line from input stream. This is copied from
   * [[org.apache.spark.util.Utils]], but it catches and logs exceptions.
   */
  private def processStreamByLineInternal(
      threadName: String,
      inputStream: InputStream,
      processLine: String => Unit): Thread = {
    val t = new Thread(threadName) {
      override def run(): Unit = {
        try {
          for (line <- Source.fromInputStream(inputStream).getLines()) {
            processLine(line)
          }
        } catch {
          case ex: Exception =>
            logDebug(s"Exception in IO listener thread $ex: ${ex.getMessage}")
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  /**
   * Runs Spark with `spark-submit`.
   * @param args the arguments to pass to Spark.
   * @param sparkHome SPARK_HOME. Sets before spawning Spark.
   * @param timeoutInSeconds timeout time in seconds.
   * @param verbose whether to enable verbose logging in Spark.
   * @return the exit code of the Spark process. 0 indicates success, nonzero indicates failure.
   */
  def runSparkSubmit(
      args: Seq[String],
      sparkHome: String,
      timeoutInSeconds: Int = 0,
      verbose: Boolean = false)(stdoutListener: String => Unit): Int = {

    val sparkSubmitFile = if (Utils.isWindows) {
      new File(s"$sparkHome\\bin\\spark-submit.cmd")
    } else {
      new File(s"$sparkHome/bin/spark-submit")
    }

    logDebug(s"Executing ${(Seq(sparkSubmitFile.getCanonicalPath) ++ args).mkString(" ")}")

    val process = Utils.executeCommand(
      Seq(sparkSubmitFile.getCanonicalPath) ++ args,
      new File(sparkHome),
      Map("SPARK_HOME" -> sparkHome))

    val stdoutThread = processStreamByLineInternal(
      s"${sparkSubmitFile.getName}-process-stdout", process.getInputStream, stdoutListener)

    var exitCodeOpt: Option[Int]= None
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
      }
      exitCodeOpt = Some(exitCode)
    } finally {
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
    // If we didn't get an exit code, something went wrong...
    exitCodeOpt.getOrElse(-1)
  }

  /**
   * Packages the classes in `clazzes` into a single JAR for use as a Spark application. Returns
   * the file representing the JAR. There are currently some constraints:
   *
   * - If the list contains a JAR, it can be the only element in the list.
   * - If the list contains classes, all classes must be at the same path.
   */
  def createSparkAppJar(clazzes: Seq[Class[_]]): File = {
    val tempDir = Utils.createTempDir()
    val classPaths = clazzes.map(c =>
      new File(c.getProtectionDomain.getCodeSource.getLocation.getPath))
    if (classPaths.size == 1 && classPaths(0).getName.endsWith(".jar")) {
      // Single program is running from a jar package, use it directly
      classPaths(0)
    } else {
      // Should only have source files, not JARs.
      assert(!classPaths.exists(_.getName.endsWith(".jar")), classPaths)
      val prefixDir = clazzes(0).getPackage.getName.replace(".", "/")
      // Prefix directory should be same for all files.
      assert(clazzes.forall(_.getPackage.getName.replace(".", "/") == prefixDir))
      // Program is running with a directory classpath, package target class to a jar
      val filesToPackage = clazzes.zip(classPaths).flatMap { case (clazz, classPath) =>
        val className = clazz.getSimpleName.stripSuffix("$")
        val files = FileUtils.listFiles(
          classPath,
          new WildcardFileFilter(s"*$className*"),
          DirectoryFileFilter.INSTANCE)
        files.asScala.toSeq
      }
      val jarFile = new File(tempDir, "sparkAppJar-%s.jar".format(System.currentTimeMillis()))
      TestUtils.createJar(filesToPackage, jarFile, Some(prefixDir))
      jarFile
    }
  }

  /**
   * Ensures that the directory at `path` exists (by creating it and any parent directories if it
   * does not).
   */
  @throws[java.io.IOException]
  def ensureDirectoryExists(path: Path): Unit = {
    val fs = FileSystem.get(new Configuration())
    if (!fs.mkdirs(path)) {
      throw new java.io.IOException(s"Could not create directory: $path")
    }
  }

  /**
   * Make a file at the given path, recursively creating any required directories along the way.
   */
  @throws[java.io.IOException]
  def makeFile(filePath: Path): FSDataOutputStream = {
    ensureDirectoryExists(filePath.getParent)
    FileSystem.get(new Configuration()).create(filePath)
  }

  /**
   * Write `str` to the file at `filePath`. If `filePath` is `None`, the string is written to a
   * temporary file. Returns the file to which the string is written.
   */
  def stringToFile(str: String, filePath: Option[Path]): File = {
    val file = filePath.map { path =>
      ensureDirectoryExists(path.getParent)
      new File(path.toString)
    }.getOrElse {
      val tmpDir = Utils.createTempDir()
      new File(tmpDir, "stringFile-%s.txt".format(System.currentTimeMillis()))
    }
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }
}
