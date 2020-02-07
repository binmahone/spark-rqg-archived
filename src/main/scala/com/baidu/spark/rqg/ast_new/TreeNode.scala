package com.baidu.spark.rqg.ast_new

trait TreeNode {

  def parent: Option[TreeNode]
  def querySession: QuerySession
  def sql: String

}
