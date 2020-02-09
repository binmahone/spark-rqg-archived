package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode, TreeNodeGenerator}

/**
 * grammar: (LIMIT (ALL | limit=expression))?
 *
 * But here we only use constant directly
 */
class LimitClause(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode {

  val limit: Int = RandomUtils.nextInt(100)
  override def sql: String = s"LIMIT $limit"
}

/**
 * LimitClause generator
 */
object LimitClause extends TreeNodeGenerator[LimitClause] {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): LimitClause = {
    new LimitClause(querySession, parent)
  }
}