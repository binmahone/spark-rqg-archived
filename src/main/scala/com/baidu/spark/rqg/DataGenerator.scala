package com.baidu.spark.rqg

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class DataGenerator(
    dbName: String, warehouse: String, tableCount: Int,
    minRowCount: Int, maxRowCount: Int,
    minColumnCount: Int, maxColumnCount: Int,
    allowedFormats: Array[String], randomSeed: Int = 0) {

  val sparkConnection: SparkConnection =
    SparkConnection.openConnection("jdbc:hive2://localhost:10000")

  val random = new Random(randomSeed)

  def populateDB(): Unit = {
    // Generate table properties
    val tables = (1 to tableCount).map { idx =>
      createRandomRQGTable(s"table_$idx")
    }

    // Create table
    tables.foreach { table =>
      prepareTableStorage(table)
      sparkConnection.createTable(table)

    }

    // TODO: generate table data
  }

  def createRandomRQGTable(tableName: String): RQGTable = {

    val location = s"$warehouse/$dbName.db/$tableName"
    val columnCount = random.nextInt(maxColumnCount - minColumnCount - 1) + minColumnCount + 1
    val columns = (1 to columnCount).map { idx =>
      val dataType =
        DataType.supportedDataTypes(random.nextInt(DataType.supportedDataTypes.length)) match {
          case s: StringType =>
            // TODO: use user-defined min/max or use random min/max within a user-defined range
            val minLength = 10
            val maxLength = 20
            s.copy(minLength = minLength, maxLength = maxLength)

          case d: DecimalType =>
            val precision = 12
            val scale = 4
            d.copy(precision = precision, scale = scale)

          case x => x
        }
      RQGColumn(s"column_$idx", dataType)
    }
    RQGTable(tableName, columns, location)
  }

  private def prepareTableStorage(table: RQGTable): Unit = {
    val path = new Path(table.location)
    val fs = path.getFileSystem(new Configuration())
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.mkdirs(path)
  }
}

object DataGenerator {

  def main(args: Array[String]): Unit = {
    // TODO: options parser
    val dbName = "rqg_test_db"
    val warehouse = "/Users/liulinhong/workspace/spark-rqg-workspace/warehouse"
    val tableCount = 5
    val minColumnCount = 3
    val maxColumnCount = 10
    val minRowCount = 400000
    val maxRowCount = 500000
    val allowedFormats = Array[String]("text")
    val seed = 10
    val dataGenerator = new DataGenerator(
      dbName, warehouse, tableCount,
      minRowCount, maxRowCount,
      minColumnCount, maxColumnCount,
      allowedFormats, seed)
    dataGenerator.populateDB()
  }
}
