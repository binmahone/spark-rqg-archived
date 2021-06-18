package org.apache.spark.rqg.comparison

import java.io.File;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rqg._
import org.apache.spark.rqg.parser.DataGeneratorOptions
import org.apache.spark.rqg.runner.{SparkQueryRunner, SparkSubmitQueryRunner}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SparkSession}

object DataGenerator extends Runner {
  def main(args: Array[String]): Unit = {
    // NOTE: workaround for derby init exception in sbt:
    // java.security.AccessControlException:
    //   access denied org.apache.derby.security.SystemPermission( "engine", "usederbyinternals" )
    System.setSecurityManager(null)

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
    val warehouse = new Path(RQGUtils.getBaseDirectory, "warehouse").toString

    // We will generate a single set of data for both ref and test system using this spark session
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()

    val refQueryRunner = new SparkSubmitQueryRunner(
      options.refSparkVersion, options.refSparkHome, options.refMaster, options.timeout,
      options.verbose)

    val dataGenerator = new DataGenerator(
      options.dryRun, options.configFile, randomizationSeed, dbName, tableCount, minRowCount,
      maxRowCount, minColumnCount, maxColumnCount, allowedDataSources.toArray, warehouse,
      sparkSession, outputDirFile, refQueryRunner)

    dataGenerator.generateData()
    sparkSession.stop()
  }
}

case class DataGenerator(
    dryRun: Boolean,
    rqgConfigPath: String,
    randomizationSeed: Int,
    dbName: String, tableCount: Int,
    minRowCount: Int, maxRowCount: Int,
    minColumnCount: Int, maxColumnCount: Int,
    allowedDataSources: Array[String], warehouse: String,
    sparkSession: SparkSession,
    loggingDir: File,
    @transient refQueryRunner: SparkQueryRunner) {

  private val BYTES_PER_BATCH = 10 * 1024 * 1024
  private val SAMPLE_ROW_COUNT = 10

  def generateData(): Unit = {
    // Step 1: Create random table attributes

    val tables = (1 to tableCount).map(idx => generateRandomRQGTable(s"table_$idx"))
    if (dryRun) {
      tables.foreach(table => println(table.prettyString))
      return
    }

    println(s"Plan to generate data for ${tables.length} random tables")

    // Create Database
    val createDatabaseSql = s"CREATE DATABASE IF NOT EXISTS $dbName"
    sparkSession.sql(createDatabaseSql)

    // Step 2: Create table metadata.
    val queries = tables.flatMap(table => toDropTableSql(table) :: toCreateTableSql(table) :: Nil)
    println("Creating table meta")
    refQueryRunner.runQueries(createDatabaseSql +: queries, new Path(loggingDir.toString), "")

    tables.foreach { rqgTable =>
      // Step 3: Estimate partition count
      val rowCount = RandomUtils.choice(minRowCount, maxRowCount)
      val bytesPerRow = TableDataGenerator.estimateBytesPerRow(rqgTable, SAMPLE_ROW_COUNT)
      val rowsPerBatch = math.max(BYTES_PER_BATCH / bytesPerRow, 1)
      val batchCount = (rowCount + rowsPerBatch - 1) / rowsPerBatch

      // Step 4: Create table meta in this program's spark session
      createTable(rqgTable)

      println(s"Generating data for ${rqgTable.name}")
      // Step 5: Generate data via spark session
      sparkSession.range(batchCount).mapPartitions({ _ =>
        // Each task should set random seed again
        RandomUtils.setSeed(randomizationSeed)
        val partitionId = TaskContext.getPartitionId()
        val partitionRowCount = math.min(rowCount - partitionId * rowsPerBatch, rowsPerBatch)
        (0 until partitionRowCount).map { _ =>
          Row.fromSeq(rqgTable.columns.map(c => {
            RandomUtils.nextValue(c.dataType)
          }))
        }.toIterator
      })(RowEncoder(rqgTable.schema)).createOrReplaceTempView(s"temp_${rqgTable.name}")
      sparkSession.sql(s"INSERT OVERWRITE TABLE ${rqgTable.dbName}.${rqgTable.name} " +
        s"SELECT * FROM temp_${rqgTable.name}")
    }

    tables.foreach(table => println(table.prettyString))
    println("Successfully generated data.")
  }

  private def toCreateTableSql(table: RQGTable): String = {
    val s = s"CREATE TABLE ${table.dbName}.${table.name} (${table.schemaString}) " +
      s"USING ${table.provider} " +
      s"LOCATION '${table.location}'"
    s
  }

  private def toDropTableSql(table: RQGTable): String = {
    s"DROP TABLE IF EXISTS ${table.dbName}.${table.name}"
  }

  private def createTable(table: RQGTable): Unit = {
    prepareTableStorage(table)
    // always drop table first
    sparkSession.sql(toDropTableSql(table))
    sparkSession.sql(toCreateTableSql(table))
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

  private def generateRandomRQGTable(tableName: String): RQGTable = {
    val provider = RandomUtils.nextChoice(allowedDataSources)
    val columnCount = RandomUtils.choice(minColumnCount, maxColumnCount)
    val config = RQGConfig.load(rqgConfigPath)
    val (_, maxNestingDepth) = config.getBound(RQGConfig.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT)
    val columns = (1 to columnCount).map { idx =>
      val dataType =
        RandomUtils.nextChoice(DataType.supportedDataTypes) match {
          case d: DecimalType =>
            val precision = RandomUtils.choice(4, 20)
            val scale = RandomUtils.choice(0, precision)
            d.copy(precision = precision, scale = scale)
          case a: ArrayType => {
            val (minNested, maxNested) = RQGConfig.load().getBound(RQGConfig.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT)
            val nestedCount = RandomUtils.choice(minNested, maxNested)
            ArrayType(RandomUtils.generateRandomSparkDataType(nestedCount))
          }
          case m: MapType =>
            val (minNested, maxNested) = RQGConfig.load().getBound(RQGConfig.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT)
            val nestedCount = RandomUtils.choice(minNested, maxNested)
            MapType(
              RandomUtils.generateRandomSparkDataType(nestedCount),
              RandomUtils.generateRandomSparkDataType(nestedCount)
            )
          case s: StructType =>
            val (minNested, maxNested) = RQGConfig.load().getBound(RQGConfig.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT)
            val nestedCountOfStruct = RandomUtils.choice(minNested, maxNested)
            val nestedCountOfFields = RandomUtils.choice(minNested, maxNested)
            StructType(RandomUtils.generateRandomStructFields(nestedCountOfStruct, nestedCountOfFields))
          case x => x
        }

      RQGColumn(s"column_$idx", dataType)
    }
    RQGTable(dbName, tableName, columns, provider, warehouse)
  }
}
