package com.baidu.spark.rqg

import scala.util.Random

case class TableDataGenerationTask(table: RQGTable, batchIdx: Int, rowCount: Int, seed: Int) {
  @transient lazy val random = new Random(seed + batchIdx)
}
