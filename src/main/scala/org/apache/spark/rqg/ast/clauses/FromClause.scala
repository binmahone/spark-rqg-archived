package org.apache.spark.rqg.ast.clauses

import org.apache.spark.rqg.ast.relations.Relation
import org.apache.spark.rqg.ast.{QueryContext, TreeNode, TreeNodeGenerator}

/**
 * fromClause
 *     : FROM relation (',' relation)* lateralView* pivotClause?
 *     ;
 *
 * For now, we support only one relation
 */
class FromClause(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends TreeNode {

  val relation: Relation = generateRelation

  private def generateRelation: Relation = {
    Relation(queryContext, Some(this))
  }

  override def sql: String = s"FROM ${relation.sql}"
}

/**
 * FromClause Generator
 */
object FromClause extends TreeNodeGenerator[FromClause] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode]): FromClause = {
    new FromClause(querySession, parent)
  }
}
