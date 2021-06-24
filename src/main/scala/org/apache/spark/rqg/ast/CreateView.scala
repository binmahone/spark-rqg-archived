package org.apache.spark.rqg.ast

import org.apache.spark.rqg.RandomUtils

class CreateView(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val viewName = s"view_${CreateView.getUniqueIdx}"
  val query: Query = Query(queryContext, Some(this))

  val viewType = RandomUtils.nextChoice(Array(ViewType.PERSISTENT, ViewType.TEMPORARY))

  override def sql: String = s"CREATE OR REPLACE " +
    s"${if (viewType == ViewType.TEMPORARY) "TEMPORARY" else ""} " +
    s"VIEW ${viewName} AS ${query.sql}"
}

object ViewType {
  val PERSISTENT = 0
  val TEMPORARY = 1
}

object CreateView extends TreeNodeGenerator[CreateView] {
  var idx = 0
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode] = None): CreateView = {
    new CreateView(querySession, parent)
  }

  def getUniqueIdx = {
    val cur_idx = idx
    idx += 1
    cur_idx
  }
}