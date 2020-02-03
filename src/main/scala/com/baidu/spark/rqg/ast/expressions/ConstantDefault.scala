package com.baidu.spark.rqg.ast.expressions

import com.baidu.spark.rqg.ast.{QuerySession, TreeNode}
import com.baidu.spark.rqg.{DataType, RandomUtils}

import org.apache.spark.internal.Logging

case class ConstantDefault(
    querySession: QuerySession,
    parent: Option[TreeNode],
    dataType: DataType[_],
    value: Any) extends PrimaryExpression {

  override def name: String = value.toString
}

object ConstantDefault extends Logging {
  def apply(
      querySession: QuerySession,
      parent: Option[TreeNode]): ConstantDefault = {

    val dataType = RandomUtils.choice(querySession.allowedDataTypes)

    logInfo(s"Generating ConstantDefault with $dataType")

    val value = RandomUtils.nextConstant(dataType)

    ConstantDefault(querySession, parent, dataType, value)
  }
}
