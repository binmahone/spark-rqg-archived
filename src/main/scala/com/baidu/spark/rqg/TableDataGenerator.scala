package com.baidu.spark.rqg

import java.io._

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object TableDataGenerator {

  private val DELIMITER = '\u0001'

  def populateOutputFile(task: TableDataGenerationTask): Unit = {
    val path = new Path(s"${task.table.location}/batch_${task.batchIdx}.data")
    val fs = path.getFileSystem(new Configuration())
    val outputStream = fs.create(path)
    generateData(task.table, outputStream, task.rowCount, task.random)
    outputStream.close()
  }

  def generateData(table: RQGTable, os: OutputStream, rowCount: Int, random: Random): Unit = {
    val valueGenerator = new ValueGenerator(random)
    for (_ <- 0 until rowCount) {
      table.columns.zipWithIndex.foreach {
        case (column, idx) =>
          if (idx > 0) {
            os.write(DELIMITER)
          }
          os.write(valueGenerator.generateValue(column.dataType).toString.getBytes())
      }
      os.write('\n')
    }
  }

  def estimateBytesPerRow(table: RQGTable, sampleRowCount: Int, random: Random): Int = {
    val outputStream = new ByteArrayOutputStream()
    generateData(table, outputStream, sampleRowCount, random)
    math.max(outputStream.toByteArray.length / sampleRowCount, 1)
  }
}
