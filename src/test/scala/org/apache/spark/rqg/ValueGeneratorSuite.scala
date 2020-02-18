package org.apache.spark.rqg

import scala.util.Random

import org.scalatest.FunSuite

class ValueGeneratorSuite extends FunSuite {

  private val seed = new Random().nextInt()
  println(s"Random seed is $seed")
  private val generator = new ValueGenerator(new Random(seed))

  test("boolean") {
    assert(generator.generateValue(BooleanType).isInstanceOf[Boolean])
  }

  test("tinyint") {
    val value = generator.generateValue(TinyIntType)
    assert(value >= Byte.MinValue && value <= Byte.MaxValue,
      s"$value is not in the range of [${Byte.MaxValue}, ${Byte.MaxValue}]")
  }

  test("smallint") {
    val value = generator.generateValue(SmallIntType)
    assert(value >= Short.MinValue && value <= Short.MaxValue,
      s"$value is not in the range of [${Short.MaxValue}, ${Short.MaxValue}]")
  }

  test("string") {
    val value = generator.generateValue(StringType)
    assert(value.length >= 0 && value.length <= StringType.MAX_LENGTH,
      s"length of $value is not in the range of [0, ${StringType.MAX_LENGTH}]")
  }

  test("decimal") {
    val precision = 15
    val scale = 8
    val bound = math.pow(10, precision - scale)
    val value = generator.generateValue(DecimalType(precision, scale))
    assert(value > -bound && value < bound,
      s"$value is not in the range of (-$bound, $bound)")
  }
}
