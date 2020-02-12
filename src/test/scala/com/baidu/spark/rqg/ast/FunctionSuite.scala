package com.baidu.spark.rqg.ast

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PrettyAttribute}

class FunctionSuite extends FunSuite {

  test("function") {
    FunctionRegistry.expressions.foreach { x =>
      println("func_name: " + x._1 + ", class: " + x._2._1.getClassName)
      try {
        val clazz = Class.forName(x._2._1.getClassName)
        clazz
          .getConstructors
          .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
          .foreach { c =>
            val count = c.getParameterCount
            val params = Seq.fill(count)(Literal(1))
            try {
              val expression = c.newInstance(params : _*).asInstanceOf[Expression]
              println(s"\tfunc: ${x._1} check input type result: ${expression.checkInputDataTypes().isSuccess}")
            } catch {
              case e: Exception =>
                println(s"\tfunc: ${x._1} with $count params is not supported")
            }
          }
      } catch {
        case e: Exception =>
          println("class: " + x._2._1.getClassName + " is not found")
      }
    }
  }
}
