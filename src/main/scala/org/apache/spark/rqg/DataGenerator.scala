package org.apache.spark.rqg

import org.apache.spark.rqg.parser.DataGeneratorOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

class DataGenerator(
    dbName: String, tableCount: Int,
    minRowCount: Int, maxRowCount: Int,
    minColumnCount: Int, maxColumnCount: Int,
    allowedDataSources: Array[String],
    sparkConnection: SparkConnection) {

  val warehouse: String = {
    new Path(FileSystem.get(new Configuration()).getHomeDirectory, "test_warehouse").toString
  }

  // TODO: get this from user input or spark config
  val sparkSession: SparkSession = SparkSession.builder().master("local[2]").getOrCreate()

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
      val rowCount = RandomUtils.choice(minRowCount, maxRowCount)
      // TODO: get this from user input
      val bytesPerBatch = 10 * 1024 * 1024
      val bytesPerRow = TableDataGenerator.estimateBytesPerRow(table, 10)
      val rowsPerBatch = math.max(bytesPerBatch / bytesPerRow, 1)
      val batchCount = (rowCount + rowsPerBatch - 1) / rowsPerBatch
      (0 until batchCount).map { batchIdx =>
        val batchRowCount = math.min(rowCount - batchIdx * rowsPerBatch, rowsPerBatch)
        TableDataGenerationTask(table, batchIdx, batchRowCount, RandomUtils.getSeed)
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

    sparkSession.stop()
  }

  def generateRandomRQGTable(tableName: String): RQGTable = {

    val provider = RandomUtils.nextChoice(allowedDataSources)

    val columnCount = RandomUtils.choice(minColumnCount, maxColumnCount)
    val columns = (1 to columnCount).map { idx =>
      val dataType =
        RandomUtils.nextChoice(DataType.supportedDataTypes) match {
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
    val options = DataGeneratorOptions.parse(args)

    val randomizationSeed = options.randomizationSeed
    RandomUtils.setSeed(randomizationSeed)

    val dbName = options.dbName
    val tableCount = options.tableCount
    val minColumnCount = options.minColumnCount
    val maxColumnCount = options.maxColumnCount
    val minRowCount = options.minRowCount
    val maxRowCount = options.maxRowCount
    val allowedDataSources = options.dataSources.map(_.toString)

    val sparkConnection =
      SparkConnection.openConnection(s"jdbc:hive2://${options.refHost}:${options.refPort}")

    val dataGenerator = new DataGenerator(
      dbName, tableCount,
      minRowCount, maxRowCount,
      minColumnCount, maxColumnCount,
      allowedDataSources.toArray,
      sparkConnection)

    dataGenerator.populateDB()
  }
}
