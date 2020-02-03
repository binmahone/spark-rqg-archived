package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.{DataType, RandomUtils, Utils}

case class TableReference(
    querySession: QuerySession,
    parent: Option[TreeNode],
    table: Table,
    alias: Option[String]) extends RelationPrimary {

  override def name: String = alias.getOrElse(table.name)

  override def columns: Array[Column] = table.columns

  override def dataTypes: Array[DataType[_]] = table.columns.map(_.dataType)

  override def sql: String = s"${table.name} ${alias.map("AS " + _).getOrElse("")}"
}

object TableReference {
  // Random creation
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): TableReference = {

    val table = RandomUtils.choice(querySession.availableTables)
    val alias = Some(Utils.nextAlias(table.name))
    TableReference(querySession, parent, table, alias)
  }
}
