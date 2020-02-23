package org.apache.spark.rqg

trait DataType[T] extends WeightedChoice {
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

case object StringType extends DataType[String] {
  val MAX_LENGTH = 32

  def typeName = "string"
}

case class DecimalType(precision: Int = 10, scale: Int = 0) extends FractionalType[Double] {
  require(scale <= precision && precision <= DecimalType.MAX_PRECISION)

  val bound = math.pow(10, precision).toLong
  val fractional = math.pow(10, scale)

  override def typeName: String = s"decimal($precision,$scale)"

  override def weightName: String = "decimal"

  override def sameType(other: DataType[_]): Boolean = other.isInstanceOf[DecimalType]
}

object DecimalType {
  val MAX_PRECISION = 38
}
