package com.baidu.spark.rqg.ast

import org.apache.spark.internal.Logging

trait TreeNode extends Logging {

  def querySession: QuerySession

  def parent: Option[TreeNode]

  def sql: String
}
