package org.apache.spark.rqg.comparison

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util._
import org.scalactic.TripleEquals._
import org.scalactic.Tolerance._

case class ResultComparator(ignoreOrder: Boolean = true, tolerance: Float = 0) {

  def checkAnswer(actual: Seq[Row], expected: Seq[Row]): Option[String] = {
    val preparedActual = prepareAnswer(actual, ignoreOrder)
    val preparedExpected = prepareAnswer(expected, ignoreOrder)

    val areEqual = if (preparedActual.length != preparedExpected.length) {
      false
    } else {
      preparedActual.zip(preparedExpected).forall { case (row1, row2) =>
        row1.length == row2.length && row1.toSeq.zip(row2.toSeq).forall {
          case (a: Float, b: Float) =>
            if (tolerance != 0) a === b +- tolerance else a == b
          case (a: Double, b: Double) =>
            if (tolerance != 0) a === b +- tolerance else a == b
          case (a: BigDecimal, b: BigDecimal) =>
            if (tolerance != 0) a === b +- BigDecimal(tolerance) else a == b
          case (a, b) => a == b
        }
      }
    }

    if (!areEqual) {
      val errorMessage =
        s"""
           |== Results ==
           |${sideBySide(
          s"== Expected Answer - ${expected.size} ==" +:
            preparedExpected.map(_.toString()),
          s"== Actual Answer - ${actual.size} ==" +:
            preparedActual.map(_.toString())).mkString("\n")}
        """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }

  private def prepareAnswer(answer: Seq[Row], ignoreOrder: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (ignoreOrder) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  private def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }
}

