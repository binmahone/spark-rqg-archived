package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast._
import com.baidu.spark.rqg.ast.relations.RelationPrimary
import com.baidu.spark.rqg.{DataType, RandomUtils, Utils}

import org.apache.spark.internal.Logging

case class ColumnReference(
    querySession: QuerySession,
    parent: Option[TreeNode],
    relation: RelationPrimary,
    column: Column) extends PrimaryExpression {

  override def dataType: DataType[_] = column.dataType

  override def name: String = column.name

  override def sql: String = s"${relation.name}.$name"
}

object ColumnReference extends Logging {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): ColumnReference = {

    val allowedRelations =
      Utils.allowedRelations(querySession.availableRelations, querySession.allowedDataTypes)

    val relation = RandomUtils.choice(allowedRelations)

    val allowedColumns = Utils.allowedColumns(relation.columns, querySession.allowedDataTypes)

    val column = RandomUtils.choice(allowedColumns)

    logInfo(s"Generating ColumnReference with ${column.dataType}")

    ColumnReference(querySession, parent, relation, column)
  }
}
