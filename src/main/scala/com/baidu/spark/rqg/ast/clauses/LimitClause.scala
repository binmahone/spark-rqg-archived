package com.baidu.spark.rqg.ast.clauses

import com.baidu.spark.rqg.RandomUtils
import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}

case class LimitClause(
    querySession: QuerySession,
    parent: Option[TreeNode],
    limit: Int) extends TreeNode {
  
  override def sql: String = s"LIMIT $limit"
}

object LimitClause {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): LimitClause = {

    LimitClause(querySession, parent, RandomUtils.nextInt(100))
  }
}
