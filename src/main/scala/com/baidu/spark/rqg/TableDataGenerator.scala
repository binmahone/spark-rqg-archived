package com.baidu.spark.rqg

import java.io._

import scala.util.Random

object TableDataGenerator {

  private val DELIMITER = '\u0001'

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
