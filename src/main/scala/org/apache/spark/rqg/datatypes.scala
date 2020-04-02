package org.apache.spark.rqg

import org.apache.spark.sql.{types => sparktypes}

trait DataType[T] extends WeightedChoice {
  def sparkType: sparktypes.DataType
  def typeName: String
  def sameType(other: DataType[_]): Boolean = this == other
  def acceptsType(other: DataType[_]): Boolean = sameType(other)
  override def weightName: String = typeName
}

trait NumericType[T] extends DataType[T] {
  override def acceptsType(other: DataType[_]): Boolean = other.isInstanceOf[NumericType[_]]
}

trait IntegralType[T] extends NumericType[T] {
  override def acceptsType(other: DataType[_]): Boolean = other.isInstanceOf[IntegralType[_]]
}

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
    StringType,
    DecimalType()
  )

  val joinableDataTypes: Array[DataType[_]] = Array(
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType,
    DecimalType()
  )
}

case object BooleanType extends DataType[Boolean] {
  override def typeName = "boolean"

  override def sparkType: sparktypes.DataType = sparktypes.BooleanType
}
case object IntType extends IntegralType[Int] {
  override def typeName = "int"

  override def sparkType: sparktypes.DataType = sparktypes.IntegerType
}
case object TinyIntType extends IntegralType[Byte] {
  override def typeName = "tinyint"

  override def sparkType: sparktypes.DataType = sparktypes.ByteType
}
case object SmallIntType extends IntegralType[Short] {
  override def typeName = "smallint"

  override def sparkType: sparktypes.DataType = sparktypes.ShortType
}
case object BigIntType extends IntegralType[Long] {
  override def typeName = "bigint"

  override def sparkType: sparktypes.DataType = sparktypes.LongType
}

case object FloatType extends FractionalType[Float] {
  override def typeName = "float"

  override def sparkType: sparktypes.DataType = sparktypes.FloatType
}
case object DoubleType extends FractionalType[Double] {
  override def typeName = "double"

  override def sparkType: sparktypes.DataType = sparktypes.DoubleType
}

case object StringType extends DataType[String] {
  val MAX_LENGTH = 32

  override def typeName = "string"

  override def sparkType: sparktypes.DataType = sparktypes.StringType
}

case object NullType extends DataType[Null] {
  override def typeName = "null"

  override def sparkType: sparktypes.DataType = sparktypes.NullType
}

case class DecimalType(precision: Int = 10, scale: Int = 0) extends FractionalType[BigDecimal] {
  require(scale <= precision && precision <= DecimalType.MAX_PRECISION)

  val bound = math.pow(10, precision).toLong
  val fractional = math.pow(10, scale)

  override def typeName: String = s"decimal($precision,$scale)"

  override def weightName: String = "decimal"

  override def sameType(other: DataType[_]): Boolean = other.isInstanceOf[DecimalType]

  override def sparkType: sparktypes.DataType = sparktypes.DecimalType(precision, scale)
}

object DecimalType {
  val MAX_PRECISION = 38
}
