package org.apache.spark.rqg

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object RQGUtils {

  def getBaseDirectory: Path = {
    new Path(FileSystem.get(new Configuration()).getHomeDirectory, "spark_rqg")
  }
}
