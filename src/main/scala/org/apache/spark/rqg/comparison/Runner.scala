package org.apache.spark.rqg.comparison

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.rqg.comparison.QueryGenerator.{log4jFile, outputDirFile}

/**
 * Class that represents a runnable object with a `main()` method. Provides common functionality
 * around setting up logging etc.
 */
abstract class Runner extends Logging {

  // A unique session name for this run.
  protected val sessionName = s"rqg-${System.currentTimeMillis()}"

  // The output directory under which all logs for this run of the RQG will be written. This is
  // $HOME/rqg_output/rqg-<uuid>, where the UUID is unique across runs.
  protected val outputDirFile: File = {
    val homeDir = System.getProperty("user.home")
    new File(new File(new File(homeDir), "rqg_output"), sessionName)
  }

  protected val log4jFile: File = new File(outputDirFile, "log4j.log")

  //  Initialize logging. This must be the first code that runs on startup.
  if (!outputDirFile.mkdirs()) {
    throw new java.io.IOException(
      s"Could not create RQG output directory ${outputDirFile.toString}")
  }
  // initializing the logger.
  System.setProperty("logfile.name", log4jFile.toString)
}
