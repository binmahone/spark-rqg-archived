package org.apache.spark.rqg

import com.typesafe.config.ConfigValueFactory
import org.apache.spark.rqg.ast.{Column, Query, QueryContext, Table}
import org.scalatest.{BeforeAndAfter, FunSuite}

class QueryGeneratorSuite extends FunSuite with BeforeAndAfter {
  before {
    RandomUtils.setSeed(1, true)
  }

  def generateTable = {
    val columnInt = Column("rqg_table", "column_int", IntType)
    val columnString = Column("rqg_table", "column_string", StringType)
    val columnDouble = Column("rqg_table", "column_double", DoubleType)
    val columnBoolean = Column("rqg_table", "column_boolean", BooleanType)
    Table("rqg_table", Array(columnInt, columnString, columnDouble, columnBoolean))
  }

  test("basic test") {
    new QueryGenerator(Array(generateTable)).createQuery().sql
  }

  test("having clause") {
    val config = RQGConfig.load("rqg-defaults.conf")
      .withValue(RQGConfig.GROUP_BY.key, ConfigValueFactory.fromAnyRef(1))
      .withValue(RQGConfig.HAVING.key, ConfigValueFactory.fromAnyRef(1))

    val querySQL = Query(QueryContext(rqgConfig = config, availableTables = Array(generateTable))).sql
    assert(querySQL.contains("HAVING"))
  }
}
