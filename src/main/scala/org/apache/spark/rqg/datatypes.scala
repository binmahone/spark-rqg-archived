package org.apache.spark.rqg

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{types => sparktypes}

import scala.collection.mutable

trait DataType[T] extends WeightedChoice {
  def sparkType: sparktypes.DataType
  def typeName: String
  def sameType(other: DataType[_]): Boolean = this == other
  def acceptsType(other: DataType[_]): Boolean = sameType(other)
  override def weightName: String = typeName
}

trait ComplexType[T] extends DataType[T] {
  override def acceptsType(other: DataType[_]): Boolean = this.sparkType.sameType(other.sparkType)
}

trait NumericType[T] extends DataType[T] {
  override def acceptsType(other: DataType[_]): Boolean = other.isInstanceOf[NumericType[_]]
}

trait IntegralType[T] extends NumericType[T] {
  override def acceptsType(other: DataType[_]): Boolean = other.isInstanceOf[IntegralType[_]]
}

trait FractionalType[T] extends NumericType[T]

object DataType {
  val primitiveSparkDataTypes: Array[sparktypes.DataType] = Array(
    sparktypes.IntegerType,
    sparktypes.StringType,
    sparktypes.DoubleType,
  )

  val complexSparkDataTypes = Array(
    sparktypes.ArrayType,
    sparktypes.StructType,
    sparktypes.MapType
  )
  val supportedDataTypes: Array[DataType[_]] = Array(
    BooleanType,
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType,
    DecimalType(),
    DateType,
    TimestampType,
    ArrayType(),
    MapType(),
    StructType()
  )

  val joinableDataTypes: Array[DataType[_]] = Array(
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType,
    DecimalType(),
    DateType,
    TimestampType
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

case object DateType extends DataType[Date] {
  override def typeName = "date"

  override def sparkType: sparktypes.DataType = sparktypes.DateType
}

case object TimestampType extends DataType[Timestamp] {
  override def typeName = "timestamp"

  override def sparkType: sparktypes.DataType = sparktypes.TimestampType
}

case object StringType extends DataType[String] {
  val MAX_LENGTH = 32

  override def typeName = "string"

  override def sparkType: sparktypes.DataType = sparktypes.StringType
}

/**
 * Represent an Spark Array with specified element type
 * @param innerType the type of each element in the array. will default to random if not specified
 */
case class ArrayType(innerType: sparktypes.DataType = RandomUtils.generateRandomSparkDataType(2))
  extends ComplexType[Array[_]] {

  override def sparkType: sparktypes.DataType = sparktypes.ArrayType(innerType)

  override def typeName: String = "array"
}

/**
 * Represent a Spark Map with specified key and value type
 * @param keyType the type of the map key. will default to random if not specified
 * @param valueType the type of the map val. will default to random if not specified
 */
case class MapType(
    keyType: sparktypes.DataType = RandomUtils.generateRandomSparkDataType(2, true),
    valueType: sparktypes.DataType = RandomUtils.generateRandomSparkDataType(2))
  extends ComplexType[mutable.Map[_, _]] {

  override def sparkType: sparktypes.DataType = sparktypes.MapType(keyType, valueType)

  override def typeName: String = "map"
}

/**
 * Represent a Spark Struct. Data is represented as Row
 * @param fields array of struct fields specifying the type of each struct element
 */
case class StructType(
    fields: Array[sparktypes.StructField] = RandomUtils.generateRandomStructFields(1, 2))
  extends ComplexType[sparktypes.StructType] {

  override def sparkType: sparktypes.DataType = sparktypes.StructType(fields)

  override def typeName: String = "struct"
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
