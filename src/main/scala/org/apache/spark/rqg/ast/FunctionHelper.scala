package org.apache.spark.rqg.ast

import org.apache.spark.rqg._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.{types => sparktypes}

import scala.collection.mutable.{ArrayBuffer, Map}

class Dummy(chosenType: sparktypes.DataType) extends Expression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = 0

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: sparktypes.DataType = sparktypes.ArrayType(sparktypes.IntegerType)

  override def children: Seq[Expression] = ???

  override def productElement(n: Int): Any = 0

  override def productArity: Int = 0

  override def canEqual(that: Any): Boolean = false
}

object FunctionHelper {
  val random = RandomUtils.getRandom
  val valueGenerator = RandomUtils.getValueGenerator

  def getArgs(paramTypes: Array[Class[_]], dataType: sparktypes.DataType): ArrayBuffer[Object] = {
    var args = ArrayBuffer[Object]()
    paramTypes.foreach(p => {
      if (p == classOf[Expression]) {
        args += new Dummy(dataType)
      } else if (p == classOf[Seq[_]]) {
        args += Seq(new Dummy(dataType))
      } else if (p == classOf[Int]) {
        args += valueGenerator.generateValue(IntType).asInstanceOf[Object]
      } else if (p == classOf[Long]) {
        args += valueGenerator.generateValue(BigIntType).asInstanceOf[Object]
      } else if (p == classOf[Double]) {
        args += valueGenerator.generateValue(DoubleType).asInstanceOf[Object]
      } else if (p == classOf[Char]) {
        args += random.nextPrintableChar().asInstanceOf[Object]
      } else if (p == classOf[String]) {
        args += valueGenerator.generateValue(StringType).asInstanceOf[Object]
      } else if (p == classOf[Option[_]]) {
        args += None
      } else if (p == classOf[StructType]) {
        args += StructType(Array(
          sparktypes.StructField("a", sparktypes.StringType, false),
          sparktypes.StructField("a1", sparktypes.StringType, false)
          )
        )
      } else if (p == classOf[Map[_, _]]) {
        args += Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
      }
    })
    args
  }
}
