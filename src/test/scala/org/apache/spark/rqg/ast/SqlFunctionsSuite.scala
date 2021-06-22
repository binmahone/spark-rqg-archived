package org.apache.spark.rqg.ast

import org.apache.spark.rqg.ast.expressions.{Constant, Expression, FunctionCall}
import org.apache.spark.rqg.{DataType, GenericNamedType, IntType, RQGConfig, RandomUtils}
import org.apache.spark.rqg.parser.QueryGeneratorOptions
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
 * Tests construction of SQL functions by creating a query that contains each function and
 * analyzing it.
 */
class SqlFunctionsSuite extends FunSuite {

  private val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
  RandomUtils.setSeed(0)

  /**
   * Function call class used for testing. Overrides the `generateArguments` method to only generate
   * constants.
   */
  class TestFunctionCall(override val func: Function)
    extends FunctionCall(QueryContext(), None, func.returnType, false) {

    override def generateArguments: Seq[Expression] = {
      // Generics should be resolved by this point.
      require(!func.returnType.isInstanceOf[GenericNamedType])
      require(!func.inputTypes.exists(_.isInstanceOf[GenericNamedType]))
      val previous = queryContext.aggPreference
      if (func.isAgg) queryContext.aggPreference = AggPreference.FORBID
      val length = func.inputTypes.length
      val arguments = (func).inputTypes.zipWithIndex.map {
        case (dt, idx) =>
          Constant(queryContext, Some(this), dt, isLast = idx == (length - 1))
      }
      if (previous != AggPreference.FORBID) queryContext.aggPreference = AggPreference.ALLOW
      arguments
    }

    override def toString = {
      s"${func.name}(${func.inputTypes.mkString(",")}) -> ${func.returnType}"
    }
  }

  Functions.getFunctions.sortBy(_.name).zipWithIndex.foreach { case (func, i) =>
    test(s"${func.toString} - $i") {
      // Use integer for generics.
      val funcSql = new TestFunctionCall(Functions.resolveGenerics(func, DataType.allDataTypes, IntType))
      sparkSession.sql(s"SELECT (${funcSql.sql})").queryExecution.assertAnalyzed()
    }
  }
}
