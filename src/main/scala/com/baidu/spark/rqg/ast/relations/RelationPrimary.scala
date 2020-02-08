package com.baidu.spark.rqg.ast.relations

import com.baidu.spark.rqg.{DataType, RandomUtils}
import com.baidu.spark.rqg.ast.{Column, QuerySession, RelationPrimaryGenerator, TreeNode}

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

    RandomUtils.choice(choices).apply(querySession, parent)
  }

  def choices = Array(TableReference)
}
