package com.baidu.spark.rqg

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class DataGenerator(
    dbName: String, warehouse: String, tableCount: Int,
    minRowCount: Int, maxRowCount: Int,
    minColumnCount: Int, maxColumnCount: Int,
    allowedDataSources: Array[String], randomSeed: Int = 0) {

  val sparkConnection: SparkConnection =
    SparkConnection.openConnection("jdbc:hive2://localhost:10000")

  // TODO: get this from user input or spark config
  val sparkSession: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

  val random = new Random(randomSeed)

  private val TEMP_TABLE_SUFFIX = "_hive_temp"

  def populateDB(): Unit = {
    // Generate table properties
    val tables = (1 to tableCount).map { idx =>
      generateRandomRQGTable(s"table_$idx")
    }

    // Create temporary hive table
    val bulkLoadTables = tables.map { table =>
      if (table.provider != "hive") {
        table.copy(name = table.name + TEMP_TABLE_SUFFIX, provider = "hive")
      } else {
        table
      }
    }

    bulkLoadTables.foreach { table =>
      prepareTableStorage(table)
      createTable(table)
    }

    // Generate table date to each table location.
    val tasks = bulkLoadTables.flatMap { table =>
      val rowCount = random.nextInt(maxRowCount - minRowCount + 1) + minRowCount
      // TODO: get this from user input
      val bytesPerBatch = 10 * 1024 * 1024
      val bytesPerRow = TableDataGenerator.estimateBytesPerRow(table, 10, random)
      val rowsPerBatch = math.max(bytesPerBatch / bytesPerRow, 1)
      val batchCount = (rowCount + rowsPerBatch - 1) / rowsPerBatch
      (0 until batchCount).map { batchIdx =>
        val batchRowCount = math.min(rowCount - batchIdx * rowsPerBatch, rowsPerBatch)
        TableDataGenerationTask(table, batchIdx, batchRowCount, randomSeed)
      }
    }

    // TODO: config spark logs
    sparkSession.sparkContext
      .parallelize(tasks, tasks.length)
      .foreach(TableDataGenerator.populateOutputFile)

    // Load data from temp hive table to target table if necessary
    tables.filter(_.provider != "hive")
      .foreach { table =>
        val tempTable = table.copy(name = table.name + TEMP_TABLE_SUFFIX, provider = "hive")
        prepareTableStorage(table)
        createTable(table)
        sparkConnection.runQuery(
          s"INSERT OVERWRITE TABLE ${table.dbName}.${table.name} " +
            s"SELECT * FROM ${tempTable.dbName}.${tempTable.name}")
        dropTable(tempTable)
        cleanupTableStorage(tempTable)
      }
  }

  def generateRandomRQGTable(tableName: String): RQGTable = {

    val provider = allowedDataSources(random.nextInt(allowedDataSources.length))
    val columnCount = random.nextInt(maxColumnCount - minColumnCount + 1) + minColumnCount
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
    RQGTable(dbName, tableName, columns, provider, warehouse)
  }

  private def createTable(table: RQGTable): Unit = {
    // always drop table first
    dropTable(table)
    val sql = s"CREATE TABLE ${table.dbName}.${table.name} (${table.schema}) " +
      s"USING ${table.provider} " +
      s"LOCATION '${table.location}'"
    sparkConnection.runQuery(sql)
  }

  private def dropTable(table: RQGTable): Unit = {
    val sql = s"DROP TABLE IF EXISTS ${table.dbName}.${table.name}"
    sparkConnection.runQuery(sql)
  }

  private def prepareTableStorage(table: RQGTable): Unit = {
    // always cleanup table storage first
    cleanupTableStorage(table)
    val path = new Path(table.location)
    val fs = path.getFileSystem(new Configuration())
    fs.mkdirs(path)
  }

  private def cleanupTableStorage(table: RQGTable): Unit = {
    val path = new Path(table.location)
    val fs = path.getFileSystem(new Configuration())
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
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
    // val minRowCount = 400000
    // val maxRowCount = 500000
    val minRowCount = 400
    val maxRowCount = 500
    val allowedDataSources = Array[String]("parquet", "json", "hive")
    val seed = 100
    val dataGenerator = new DataGenerator(
      dbName, warehouse, tableCount,
      minRowCount, maxRowCount,
      minColumnCount, maxColumnCount,
      allowedDataSources, seed)
    dataGenerator.populateDB()
  }
}
