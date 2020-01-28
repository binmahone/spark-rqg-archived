package com.baidu.spark.rqg

import scala.util.Random

class ValueGenerator(random: Random) {

  def generateValue[T](dataType: DataType[T]): T = {
    val value = dataType match {
      case BooleanType =>
        random.nextBoolean()
      case TinyIntType =>
        (random.nextInt(Byte.MaxValue - Byte.MinValue + 1) + Byte.MinValue).toByte
      case SmallIntType =>
        (random.nextInt(Short.MaxValue - Short.MinValue + 1) + Short.MinValue).toShort
      case IntType =>
        random.nextInt()
      case BigIntType =>
        random.nextLong()
      case FloatType =>
        random.nextFloat()
      case DoubleType =>
        random.nextDouble()
      case StringType(minLength, maxLength) =>
        val length = random.nextInt(maxLength + minLength - 1) - minLength + 1
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        val sb = new StringBuilder
        for (_ <- 0 until length) {
          sb.append(chars(random.nextInt(chars.length)))
        }
        sb.toString
      case d: DecimalType =>
        (random.nextLong() % d.bound) / d.fractional
      case x =>
        // TODO: Date, Timestamp, Char, Varchar, Binary, Interval
        throw new NotImplementedError(s"data type $x not supported yet")
    }
    value.asInstanceOf[T]
  }
}