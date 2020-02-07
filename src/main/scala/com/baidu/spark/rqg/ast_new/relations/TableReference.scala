package com.baidu.spark.rqg.ast_new.relations

import com.baidu.spark.rqg.{DataType, RandomUtils}
import com.baidu.spark.rqg.ast_new._

class TableReference(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends RelationPrimary {

  val table: Table = RandomUtils.choice(querySession.availableTables)
  val alias = Some(querySession.nextAlias(table.name))

  override def sql: String = s"${table.name} ${alias.map("AS " + _).getOrElse("")}"

  override def name: String = alias.getOrElse(table.name)

  override def columns: Array[Column] = table.columns

  override def dataTypes: Array[DataType[_]] = table.columns.map(_.dataType)
}

object TableReference extends RelationPrimaryGenerator[TableReference] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): TableReference = {
    new TableReference(querySession, parent)
  }
}
