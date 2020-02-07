package com.baidu.spark.rqg.ast_new.relations

import com.baidu.spark.rqg.{DataType, RandomUtils}
import com.baidu.spark.rqg.ast_new.{Column, QuerySession, RelationPrimaryGenerator, TreeNode}

trait RelationPrimary extends TreeNode {
  def name: String

  def columns: Array[Column]

  def dataTypes: Array[DataType[_]]
}

object RelationPrimary extends RelationPrimaryGenerator[RelationPrimary] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): RelationPrimary = {

    RandomUtils.choice(choices).apply(querySession, parent)
  }

  def choices = Array(TableReference)
}