package org.apache.spark.rqg

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object TableDataGenerator {

  private val COLUMN_DELIMITER = '\u0001'

  def populateOutputFile(task: TableDataGenerationTask): Unit = {
    // This is called in spark task, we need reset the RandomUtils seed if necessary
    RandomUtils.setSeed(task.seed)

    val path = new Path(s"${task.table.location}/batch_${task.batchIdx}.data")
    val fs = path.getFileSystem(new Configuration())
    val outputStream = fs.create(path)
    generateData(task.table, outputStream, task.rowCount)
    outputStream.close()
  }

  def generateData(table: RQGTable, os: OutputStream, rowCount: Int): Unit = {
    for (_ <- 0 until rowCount) {
      table.columns.zipWithIndex.foreach {
        case (column, idx) =>
          if (idx > 0) {
            os.write(COLUMN_DELIMITER)
          }
          val res = RandomUtils.nextValue(column.dataType)
          if (res == null) {
            os.write("null".getBytes())
          } else {
            os.write(res.toString.getBytes())
          }
      }
      os.write('\n')
    }
  }

  def estimateBytesPerRow(table: RQGTable, sampleRowCount: Int): Int = {
    val outputStream = new ByteArrayOutputStream()
    generateData(table, outputStream, sampleRowCount)
    math.max(outputStream.toByteArray.length / sampleRowCount, 1)
  }
}

case class TableDataGenerationTask(table: RQGTable, batchIdx: Int, rowCount: Int, seed: Int)
