package com.baidu.spark.rqg

trait DataType[T]

case object BooleanType extends DataType[Boolean]
case object IntType extends DataType[Int]
case object TinyIntType extends DataType[Byte]
case object SmallIntType extends DataType[Short]
case object BigIntType extends DataType[Long]

case object FloatType extends DataType[Float]
case object DoubleType extends DataType[Double]

case class StringType(minLength: Int = 0, maxLength: Int = 10) extends DataType[String] {
  val MAX_LENGTH = 40
  require(minLength >= 0 && maxLength <= MAX_LENGTH)
}

case class DecimalType(precision: Int = 10, scale: Int = 0) extends DataType[Double] {
  val MAX_PRECISION = 38
  require(scale <= precision && precision <= MAX_PRECISION)

  val bound = math.pow(10, precision).toLong
  val fractional = math.pow(10, scale)
}

