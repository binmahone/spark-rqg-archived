package org.apache.spark.rqg.ast.relations

import org.apache.spark.rqg.{DataType, RandomUtils}
import org.apache.spark.rqg.ast.{Column, QuerySession, RelationPrimaryGenerator, TreeNode}

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

  def columns: Array[Column]

  def dataTypes: Array[DataType[_]]
}

/**
 * This is the companion object used to generate a RelationPrimary. For now we only support
 * TableReference and will support AliasedQuery and FunctionTable in the future.
 */
object RelationPrimary extends RelationPrimaryGenerator[RelationPrimary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): RelationPrimary = {

    RandomUtils.nextChoice(choices).apply(querySession, parent)
  }

  def choices = Array(TableReference)
}
