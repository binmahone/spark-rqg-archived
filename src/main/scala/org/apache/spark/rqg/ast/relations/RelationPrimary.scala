package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{DataType, RandomUtils, StructType}
import org.apache.spark.rqg.ast.{Column, QueryContext, RelationPrimaryGenerator, TreeNode}

/**
 * relationPrimary
 *   : multipartIdentifier sample? tableAlias  #tableName
 *   | '(' query ')' sample? tableAlias        #aliasedQuery
 *   | '(' relation ')' sample? tableAlias     #aliasedRelation
 *   | inlineTable                             #inlineTableDefault2
 *   | functionTable                           #tableValuedFunction
 *   ;
 */
trait RelationPrimary extends TreeNode {
  def name: String
  def dataTypes: Array[DataType[_]]
  val columns: Array[Column]

  lazy val flattenedColumns: Array[Column] = flattenNestedColumns(columns)

  /// Returns all columns in this relation, including the ones that are part of nested structs.
  private def flattenNestedColumns(columns: Array[Column]): Array[Column] = {
    columns.flatMap {
      case col @ Column(_, _, StructType(fields)) =>
        val asColumns = fields.map(field => Column(col.sql, field.name, field.dataType))
        // include the struct itself as a valid column.
        Seq(col) ++ flattenNestedColumns(asColumns)
      case other => Array(other)
    }
  }
}

/**
 * This is the companion object used to generate a RelationPrimary. For now we only support
 * TableReference and will support AliasedQuery and FunctionTable in the future.
 */
object RelationPrimary extends RelationPrimaryGenerator[RelationPrimary] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): RelationPrimary = {
    RandomUtils.nextChoice(choices).apply(querySession, parent)
  }

  def choices = Array(TableReference)
}
