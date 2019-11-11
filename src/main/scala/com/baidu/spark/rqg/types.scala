package com.baidu.spark.rqg

import java.util.Locale

abstract class DataType {
  def getBaseType: DataType = {
    // NOTE: Tricky way to get a same implementation with impala RQG
    var clazz: Class[_] = this.getClass
    while (!clazz.getSuperclass.equals(classOf[DataType])) {
      clazz = clazz.getSuperclass
    }
    val dt = Class.forName(clazz.getCanonicalName + "$").getField("MODULE$").get(null)
    dt.asInstanceOf[DataType]
  }

  def getGenericType: DataType = getBaseType

  def getType: DataType = {
    this match {
      case CharType | DataType | DecimalType | FloatType | IntType | NumberType | TimestampType => this
      case _ => getGenericType
    }
  }

  def typeName: String = {
    this.getClass.getSimpleName
      .stripSuffix("$").stripSuffix("Type")
      .toLowerCase(Locale.ROOT)
  }
}

object DataType extends DataType {
  val EXACT_TYPES = Set(
    BigIntType,
    BooleanType,
    // CharType,
    DecimalType,
    DoubleType,
    FloatType,
    IntType,
    SmallIntType,
    TimestampType,
    VarCharType)

  val TYPES: Set[DataType] = EXACT_TYPES.map(_.getType)

  val JOINABLE_TYPES = Set(
    CharType,
    DecimalType,
    TimestampType,
    IntType
  )

  def nameToType(name: String): DataType = {
    TYPES.find(_.typeName == name).getOrElse {
      throw new IllegalArgumentException(s"Failed to covert $name to data type")
    }
  }
}

class BigIntType private() extends IntType
case object BigIntType extends BigIntType

class BooleanType private() extends DataType
case object BooleanType extends BooleanType

class CharType protected() extends DataType
case object CharType extends CharType

class NumberType protected() extends DataType
case object NumberType extends NumberType

class DecimalType private() extends NumberType {
  override def getGenericType: DataType = DecimalType
}
case object DecimalType extends DecimalType

class DoubleType private() extends FloatType
case object DoubleType extends DoubleType

class FloatType protected() extends NumberType {
  override def getGenericType: DataType = FloatType
}
case object FloatType extends FloatType

class IntType protected() extends NumberType {
  override def getGenericType: DataType = IntType
}
case object IntType extends IntType

class SmallIntType private() extends IntType
case object SmallIntType extends SmallIntType

class StringType private() extends VarCharType {
  val MAX = 255
}
case object StringType extends StringType

class VarCharType protected() extends CharType
case object VarCharType extends VarCharType

class TimestampType private() extends DataType
case object TimestampType extends TimestampType

object App {

  def getBaseType(dataType: DataType): DataType = {
    var clazz: Class[_] = dataType.getClass
    while (!clazz.getSuperclass.equals(classOf[DataType])) {
      clazz = clazz.getSuperclass
    }
    val dt = Class.forName(clazz.getCanonicalName + "$").getField("MODULE$").get(null)
    dt.asInstanceOf[DataType]
  }

  def main(args: Array[String]): Unit = {
    println(BigIntType.getBaseType)
    println(BooleanType.getBaseType)
    println(CharType.getBaseType)
    println(DecimalType.getBaseType)
    println(DoubleType.getBaseType)
    println(FloatType.getBaseType)
    println(IntType.getBaseType)
    println(SmallIntType.getBaseType)
    println(TimestampType.getBaseType)
    println(VarCharType.getBaseType)
    println("=================")
    println(BigIntType.getGenericType)
    println(BooleanType.getGenericType)
    println(CharType.getGenericType)
    println(DecimalType.getGenericType)
    println(DoubleType.getGenericType)
    println(FloatType.getGenericType)
    println(IntType.getGenericType)
    println(SmallIntType.getGenericType)
    println(TimestampType.getGenericType)
    println(VarCharType.getGenericType)
    println("=================")
    println(BigIntType.getType)
    println(BooleanType.getType)
    println(CharType.getType)
    println(DecimalType.getType)
    println(DoubleType.getType)
    println(FloatType.getType)
    println(IntType.getType)
    println(SmallIntType.getType)
    println(TimestampType.getType)
    println(VarCharType.getType)
    println("=================")
    println(DataType.EXACT_TYPES)
    println(DataType.TYPES)
  }
}
