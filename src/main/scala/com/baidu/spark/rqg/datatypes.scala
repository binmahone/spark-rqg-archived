package com.baidu.spark.rqg

trait DataType[T] {
  def typeName: String
}
trait NumericType[T] extends DataType[T]
trait IntegralType[T] extends NumericType[T]
trait FractionalType[T] extends NumericType[T]

object DataType {
  val supportedDataTypes: Array[DataType[_]] = Array(
    BooleanType,
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType(),
    DecimalType()
  )

  val joinableDataTypes: Array[DataType[_]] = Array(
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType(),
    DecimalType()
  )
}

case object BooleanType extends DataType[Boolean] {
  def typeName = "boolean"
}
case object IntType extends IntegralType[Int] {
  def typeName = "int"
}
case object TinyIntType extends IntegralType[Byte] {
  def typeName = "tinyint"
}
case object SmallIntType extends IntegralType[Short] {
  def typeName = "smallint"
}
case object BigIntType extends IntegralType[Long] {
  def typeName = "bigint"
}

case object FloatType extends FractionalType[Float] {
  def typeName = "float"
}
case object DoubleType extends FractionalType[Double] {
  def typeName = "double"
}

case class StringType(minLength: Int = 0, maxLength: Int = 10) extends DataType[String] {
  val MAX_LENGTH = 256
  require(minLength >= 0 && maxLength <= MAX_LENGTH)

  def typeName = "string"
}

case class DecimalType(precision: Int = 10, scale: Int = 0) extends FractionalType[Double] {
  val MAX_PRECISION = 38
  require(scale <= precision && precision <= MAX_PRECISION)

  val bound = math.pow(10, precision).toLong
  val fractional = math.pow(10, scale)

  override def typeName: String = s"decimal"
}
