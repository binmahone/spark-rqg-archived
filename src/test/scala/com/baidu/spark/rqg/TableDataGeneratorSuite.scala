package com.baidu.spark.rqg

import java.io.ByteArrayOutputStream

import scala.util.Random

import org.scalatest.FunSuite

class TableDataGeneratorSuite extends FunSuite {

  test("basic") {
    val rowCount = 100
    val columnInt = RQGColumn("column_int", IntType)
    val columnString = RQGColumn("column_string", StringType)
    val columnDecimal = RQGColumn("column_decimal", DecimalType(10, 5))
    val table = RQGTable("rqg_db", "rqg_table", Seq(columnInt, columnString, columnDecimal))

    val outputStream = new ByteArrayOutputStream()
    TableDataGenerator.generateData(table, outputStream, rowCount)
    val rows = new String(outputStream.toByteArray).split('\n')
    assert(rows.length == rowCount)
    rows.foreach { row =>
      assert("^(-?[0-9]*)\u0001([0-9a-zA-Z]*)\u0001(-?[0-9.]*)$".r.findFirstMatchIn(row).isDefined)
    }
  }
}
