package org.apache.spark.rqg.ast

import org.apache.spark.rqg._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.{types => sparktypes}

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * A dummy expression that has the given return type.
 */
class Dummy(chosenType: sparktypes.DataType) extends Expression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = 0

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: sparktypes.DataType = chosenType

  override def children: Seq[Expression] = Seq()

  override def productElement(n: Int): Any = 0

  override def productArity: Int = 0

  override def canEqual(that: Any): Boolean = false
}

object FunctionHelper {
  /**
   * Generates arguments that are compatible with `paramTypes`. If `dataTypes` is provided,
   * [[org.apache.spark.sql.catalyst.expressions.Expression]] arguments are set to the provided
   * types, from left to right in the order provided. This function is guaranteed to be
   * deterministic.
   */
  def getArgs(
    paramTypes: Array[Class[_]],
    dataTypes: Option[Seq[sparktypes.DataType]] = None): ArrayBuffer[Object] = {
    var args = ArrayBuffer[Object]()
    var typeIndex = 0
    val sparkDataTypes = dataTypes.getOrElse(
      Seq.fill(paramTypes.size)(sparktypes.IntegerType))
    paramTypes.foreach(p => {
      if (p == classOf[Expression]) {
        args += new Dummy(sparkDataTypes(typeIndex))
        typeIndex += 1
      } else if (p == classOf[Seq[Expression]]) {
        args += Seq(new Dummy(sparkDataTypes(typeIndex)))
        typeIndex += 1
      } else if (p == classOf[Int]) {
        // Use "large" numbers here and below since some expressions require constants to be within
        // some bound. By using a large number, we can exceed that bound and catch these errors
        // while registering functions.
        args += (999999).asInstanceOf[Object]
      } else if (p == classOf[Long]) {
        args += (999999).asInstanceOf[Object]
      } else if (p == classOf[Double]) {
        args += (999999.999999).asInstanceOf[Object]
      } else if (p == classOf[Char]) {
        args += ('?').asInstanceOf[Object]
      } else if (p == classOf[String]) {
        args += ("???").asInstanceOf[Object]
      } else if (p == classOf[Option[_]]) {
        args += None
      } else if (p == classOf[StructType]) {
        args += StructType(Array(
          sparktypes.StructField("s0", sparktypes.StringType, false),
          sparktypes.StructField("s1", sparktypes.StringType, false)
          )
        )
      } else if (p == classOf[Map[_, _]]) {
        // Create a dummy map
        args += Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
      } else if (p == classOf[Boolean]) {
        // TODO(shoumik): Generate false here since most uses of booleans in expressions is to
        //  specify whether we use ANSI/crash on error. It might be better to just specify these
        //  functions manually, however.
        args += (false).asInstanceOf[Object]
      } else {
        throw new IllegalArgumentException(s"Unknown argument type $p in function registry.")
      }
    })
    args
  }
}
