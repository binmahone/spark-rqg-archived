package com.baidu.spark.rqg

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class QueryProfileSuite extends FunSuite {
  test("chooseFromWeights test") {
    val weights = Map("a" -> 10, "b" -> 20, "c" -> 30)
    val sum = weights.values.sum
    val normalizedWeights = weights.mapValues(_.toDouble / sum)
    val profile = new QueryProfile()
    Array.fill(1200)(profile.chooseFromWeights(weights))
      .groupBy(identity).mapValues(_.length.toDouble / 1200)
      .foreach {
        case (k, v) =>
          assert(v === normalizedWeights(k) +- 0.05)
      }
  }

  test("chooseWindowType test") {
    val profile = new QueryProfile()
    Array.fill(2700)(profile.chooseWindowType)
      .groupBy(identity).mapValues(_.length.toDouble / 100)
      .foreach {
        case (k, v) =>
          assert(v === profile.weights("ANALYTIC_WINDOW")(k).toDouble +- 0.4)
      }
  }
}
