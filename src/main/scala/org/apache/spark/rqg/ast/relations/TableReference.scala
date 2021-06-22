package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{DataType, RandomUtils}
import org.apache.spark.rqg.ast._

/**
 * Represents an aliased table. For now we always generate alias for table to avoid conflict.
 */
class TableReference(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends RelationPrimary {

  val table: Table = RandomUtils.nextChoice(queryContext.availableTables)
  val alias = Some(queryContext.nextAlias("table"))

  override def sql: String = s"${table.name} ${alias.map("AS " + _).getOrElse("")}"

  override def name: String = alias.getOrElse(table.name)

  override val columns: Array[Column] =
    table.columns.map(c => Column(alias.get, c.name, c.dataType))

  override def dataTypes: Array[DataType[_]] = table.columns.map(_.dataType)
}

/**
 * TableReference generator
 */
object TableReference extends RelationPrimaryGenerator[TableReference] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): TableReference = {
    new TableReference(querySession, parent)
  }
}
