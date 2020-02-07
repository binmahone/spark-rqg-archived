package com.baidu.spark.rqg.ast.expressions_new

trait TreeNode {

  def parent: Option[TreeNode]
  def querySession: QuerySession
  def sql: String

}
