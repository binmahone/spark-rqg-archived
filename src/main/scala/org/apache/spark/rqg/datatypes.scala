package org.apache.spark.rqg

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{types => sparktypes}

import scala.collection.mutable

sealed trait DataType[T] extends WeightedChoice {
  /**
   * Returns the [[org.apache.spark.sql.types.DataType]] equivalent of this type.
   */
  def sparkType: sparktypes.DataType
  /**
   * A name that can be used as a field in a STRUCT. Should not contain characters disallowed
   * by SQL in names.
   */
  def fieldName: String

  /**
   * Returns whether this type has parameters, e.g., decimal precision or scale or a nested type.
   */
  def hasParameters: Boolean = false

  /**
   * Returns whether this type can be used in a join condition.
   */
  def isJoinable: Boolean = true

  /**
   * Returns whether this is the same as `other`.
   */
  def sameType(other: DataType[_]): Boolean = this == other

  /**
   * Weight name for the type. This should be kept in sync with [[org.apache.spark.rqg.RQGConfig]].
   * This is case-insensitive, but we capitalize by default for clarity.
   */
  override def weightName: String = fieldName.capitalize

  /**
   * Converts this type to its SQL string representation.
   */
  def toSql: String = sparkType.catalogString

  override def toString: String = toSql
}

trait GenericDataType extends DataType[String] {
  def name: String
}

/**
 * A generic type used to define functions whose input type can be any type. Two instances with the
 * same `name` are assumed to be the same type. As an example, func(A, B, A) means the first and
 * third type *must* be the same, and the second type can be anything (including, but not limited
 * to, the type 'A').
 */
case class GenericNamedType(name: String) extends GenericDataType {
  def sparkType: sparktypes.DataType = sparktypes.NullType
  def fieldName: String = name
  override def toString: String = name
  override def hasParameters: Boolean = true
  /// Not allowed.
  override def toSql: String = ???
}

/**
 * A generic array type, parameterized on the element type.
 */
case class GenericArrayType(elementType: GenericNamedType) extends GenericDataType {
  override def name = s"Array[${elementType.name}]"
  def sparkType: sparktypes.DataType = sparktypes.NullType
  def fieldName: String = name
  override def toString: String = name
  override def hasParameters: Boolean = true
  /// Not allowed.
  override def toSql: String = ???
}

trait ComplexType[T] extends DataType[T] {
  override def sameType(other: DataType[_]): Boolean = this.sparkType.sameType(other.sparkType)
}

trait NumericType[T] extends DataType[T] {
  override def sameType(other: DataType[_]): Boolean = this.sparkType.sameType(other.sparkType)
}

trait IntegralType[T] extends NumericType[T]
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

  val allDataTypes: Array[DataType[_]] = Array(
    BooleanType,
    IntType,
    TinyIntType,
    SmallIntType,
    BigIntType,
    FloatType,
    DoubleType,
    StringType,
    DateType,
    TimestampType,
    // Parameterized types. These are "filled in" as needed with concrete child types or parameters.
    StructType(Array(StructField("col1", IntType))),
    ArrayType(IntType),
    // MapType(IntType, IntType),
    DecimalType(1, 0),
  )

  /**
   * Returns all supported data types given the current configuration.
   */
  def supportedDataTypes(config: RQGConfig): Array[DataType[_]] = {
    val configDataTypes = config.getWeight(RQGConfig.QUERY_DATA_TYPE)
    val result = allDataTypes.filter { dataType =>
      configDataTypes.find(_.key.toLowerCase() == dataType.weightName.toLowerCase()) match {
        // Type is supported if it has a weight > 0.
        case Some(entry) => entry.value > 0
        case None => false
      }
    }
    result
  }

  /**
   * Converts a `sparkType` into an RQG-internal type.
   */
  def apply(sparkType: sparktypes.DataType): DataType[_] = sparkType match {
    case sparktypes.BooleanType => BooleanType
    case sparktypes.ByteType => TinyIntType
    case sparktypes.ShortType => SmallIntType
    case sparktypes.IntegerType => IntType
    case sparktypes.LongType => BigIntType
    case sparktypes.FloatType => FloatType
    case sparktypes.DoubleType => DoubleType
    case sparktypes.StringType => StringType
    case sparktypes.DateType => DateType
    case sparktypes.TimestampType => TimestampType
    case d: sparktypes.DecimalType => DecimalType(d.precision, d.scale)
    case sparktypes.ArrayType(e, _) => ArrayType(DataType(e))
    case sparktypes.MapType(k, v, _) => MapType(DataType(k), DataType(v))
    case sparktypes.StructType(fields) => StructType(fields.map(f => StructField(f.name, DataType(f.dataType))))
    case _ => throw new IllegalArgumentException(s"Unsupported Spark data type $sparkType")
  }

  def containsMap(dataType: DataType[_]): Boolean = dataType match {
    case ArrayType(innerType) => containsMap(innerType)
    case StructType(fields) => fields.exists(f => containsMap(f.dataType))
    case _: MapType => true
    case _ => false
  }
}

case object BooleanType extends DataType[Boolean] {
  override def fieldName = "boolean"
  override def sparkType: sparktypes.DataType = sparktypes.BooleanType
  // Don't join on boolean columns. This can lead to cross-join like conditions that result
  // in frequent OOMs.
  override def isJoinable: Boolean = false

}

case object IntType extends IntegralType[Int] {
  override def fieldName = "int"
  override def sparkType: sparktypes.DataType = sparktypes.IntegerType
}

case object TinyIntType extends IntegralType[Byte] {
  override def fieldName = "tinyint"
  override def sparkType: sparktypes.DataType = sparktypes.ByteType
}

case object SmallIntType extends IntegralType[Short] {
  override def fieldName = "smallint"
  override def sparkType: sparktypes.DataType = sparktypes.ShortType
}

case object BigIntType extends IntegralType[Long] {
  override def fieldName = "bigint"
  override def sparkType: sparktypes.DataType = sparktypes.LongType
}

case object FloatType extends FractionalType[Float] {
  override def fieldName = "float"
  override def sparkType: sparktypes.DataType = sparktypes.FloatType
  override def isJoinable: Boolean = false

}

case object DoubleType extends FractionalType[Double] {
  override def fieldName = "double"
  override def sparkType: sparktypes.DataType = sparktypes.DoubleType
  override def isJoinable: Boolean = false
}

case object DateType extends DataType[Date] {
  override def fieldName = "date"
  override def sparkType: sparktypes.DataType = sparktypes.DateType
}

case object TimestampType extends DataType[Timestamp] {
  override def fieldName = "timestamp"
  override def sparkType: sparktypes.DataType = sparktypes.TimestampType
}

case object StringType extends DataType[String] {
  val MAX_LENGTH = 32
  override def fieldName = "string"
  override def sparkType: sparktypes.DataType = sparktypes.StringType
}

/**
 * Represents a Spark array with specified element type.
 * @param innerType the type of each element in the array.
 */
case class ArrayType(innerType: DataType[_])
  extends ComplexType[Array[_]] {
  override def sparkType: sparktypes.DataType = sparktypes.ArrayType(innerType.sparkType)
  override def fieldName: String = "array"
  override def hasParameters: Boolean = true
  override def isJoinable: Boolean = false
}

object ArrayType {
  def apply(sparkType: sparktypes.DataType): ArrayType = {
    ArrayType(DataType(sparkType))
  }
}

/**
 * Represent a Spark Map with specified key and value type
 * @param keyType the type of the map key. will default to random if not specified
 * @param valueType the type of the map val. will default to random if not specified
 */
case class MapType(keyType: DataType[_], valueType: DataType[_])
  extends ComplexType[mutable.Map[_, _]] {

  override def sparkType: sparktypes.DataType = sparktypes.MapType(
    keyType.sparkType,
    valueType.sparkType)

  override def fieldName: String = "map"
  override def hasParameters: Boolean = true
  override def isJoinable: Boolean = false
}

object MapType {
  def apply(sparkKeyType: sparktypes.DataType, sparkValueType: sparktypes.DataType): MapType = {
    MapType(DataType(sparkKeyType), DataType(sparkValueType))
  }
}

case class StructField(name: String, dataType: DataType[_])

/**
 * Represent a Spark Struct. Data is represented as Row
 * @param fields array of struct fields specifying the type of each struct element
 */
case class StructType(fields: Array[StructField]) extends ComplexType[sparktypes.StructType] {
  override def sparkType: sparktypes.DataType =
    sparktypes.StructType(fields.map(f => sparktypes.StructField(f.name, f.dataType.sparkType)))
  override def fieldName: String = "struct"
  override def hasParameters: Boolean = true
}

object StructType {
  def apply(sparkStructFields: Array[sparktypes.StructField]): StructType = {
    StructType(sparkStructFields.map(f => StructField(f.name, DataType(f.dataType))))
  }
}

case class DecimalType(precision: Int, scale: Int) extends FractionalType[BigDecimal] {
  require(scale <= precision && precision <= DecimalType.MAX_PRECISION)
  override def fieldName: String = s"decimal"
  override def sparkType: sparktypes.DataType = sparktypes.DecimalType(precision, scale)
  override def hasParameters: Boolean = true
}

object DecimalType {
  val MAX_PRECISION = 38
}
