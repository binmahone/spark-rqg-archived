package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.{DataType, RandomUtils}
import com.baidu.spark.rqg.ast.{Column, QuerySession, TreeNode, TreeNodeGenerator}

/**
 * booleanExpression
 *   : NOT booleanExpression                                        #logicalNot
 *   | EXISTS '(' query ')'                                         #exists
 *   | valueExpression predicate?                                   #predicated
 *   | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
 *   | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
 *   ;
 *
 * The root class of all expressions defined in sqlbase.g4
 *
 * Here the name may lead some ambiguous: BooleanExpression will also generate non-boolean
 * expression.
 * for example: booleanExpression -> valueExpression -> primaryExpression -> constant
 */
// TODO: This should be a trait
class BooleanExpression(
    val querySession: QuerySession,
    val parent: Option[TreeNode]) extends TreeNode with Expression {

  val column: Column = RandomUtils.choice(querySession.availableRelations.flatMap(_.columns))

  override def name: String = column.name
  override def dataType: DataType[_] = column.dataType
  override def sql: String = column.name
}

/**
 * BooleanExpression Generator. It random generate one class extends BooleanExpression
 */
object BooleanExpression extends TreeNodeGenerator[BooleanExpression] {
  override def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): BooleanExpression = {
    // TODO: random generate a class extends BooleanExpression
    new BooleanExpression(querySession, parent)
  }
}
