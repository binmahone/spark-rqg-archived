package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.{DataType, RandomUtils}

trait RelationPrimary extends TreeNode {

  def name: String

  def columns: Array[Column]

  def dataTypes: Array[DataType[_]]
}

object RelationPrimary {

  def apply(querySession: QuerySession, parent: Option[TreeNode]): RelationPrimary = {

    RandomUtils.nextInt(1) match {
      case 0 => TableReference(querySession, parent)
    }
  }
}