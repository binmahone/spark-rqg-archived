package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.RandomUtils
import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * grammar: (LIMIT (ALL | limit=expression))?
 *
 * But here we only use constant directly
 */
class LimitClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val limit: Int = RandomUtils.nextInt(100)
  override def sql: String = s"LIMIT $limit"
}

/**
 * LimitClause generator
 */
object LimitClause extends TreeNodeGenerator[LimitClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): LimitClause = {
    new LimitClause(querySession, parent)
  }
}
