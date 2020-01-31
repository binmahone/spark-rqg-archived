package com.baidu.spark.rqg.ast

import scala.util.Random

import org.apache.spark.internal.Logging

abstract class TreeNode(querySession: QuerySession, parent: Option[TreeNode]) extends Logging {
  // TODO: visitor pattern is better
  def toSql: String

  // TODO: use a global singleton random util
  val random: Random = new Random()
}
