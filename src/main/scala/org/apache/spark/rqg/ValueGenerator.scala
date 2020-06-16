package org.apache.spark.rqg

import java.sql.{Date, Timestamp}
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.{Row, types => sparktypes}

class ValueGenerator(random: Random) {

  def getRandomEpochTime(): Long = {
    val currentTime = System.currentTimeMillis()
    val randomVal = random.nextDouble()
    val randomEpochTime = (randomVal * currentTime).toLong
    randomEpochTime
  }

  /**
   * Generate value based on spark DataType
   *
   * @param dataType The dataType
   * @return A value of Scala type depends on spark type
   */
  def generateWithSparkType(dataType: sparktypes.DataType): Any = {
    // First try to filter with primitives type
    val innerTypes = DataType.supportedDataTypes.filter(dt => dt.sparkType == dataType)
    if (!innerTypes.isEmpty) {
      // Found primitive type
      val innerType = innerTypes.head
      generateValue(innerType)
    } else {
      // Cannot find primitive type, must be complex type
      val complexType: DataType[_] = if (dataType.isInstanceOf[sparktypes.ArrayType]) {
        ArrayType(dataType.asInstanceOf[sparktypes.ArrayType].elementType)
      } else if(dataType.isInstanceOf[sparktypes.MapType]) {
        val keyType = dataType.asInstanceOf[sparktypes.MapType].keyType
        val valType = dataType.asInstanceOf[sparktypes.MapType].valueType
        MapType(keyType, valType)
      } else {
        StructType(dataType.asInstanceOf[sparktypes.StructType].fields)
      }
      generateValue(complexType)
    }
  }

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
      case DateType =>
        val date = new Date(getRandomEpochTime())
        date
      case TimestampType =>
        val timestamp = new Timestamp(getRandomEpochTime())
        timestamp
      case StringType =>
        val length = random.nextInt(StringType.MAX_LENGTH - 1) + 1
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        val sb = new StringBuilder
        for (_ <- 0 until length) {
          sb.append(chars(random.nextInt(chars.length)))
        }
        sb.toString
      case d: DecimalType =>
        BigDecimal((random.nextLong() % d.bound) / d.fractional)
      case a: ArrayType =>
        val randomLength = random.nextInt(5) + 1
        (0 to randomLength).toArray.map(_ => generateWithSparkType(a.innerType))
      case m: MapType =>
        val randomLength = random.nextInt(5) + 1
        // generate two lists of key and value based the their types
        val keyList = (0 to randomLength).toArray.map(_ => generateWithSparkType(m.keyType))
        val valList = (0 to randomLength).toArray.map(_ => generateWithSparkType(m.valueType))

        val res = mutable.Map[T, T]()

        for (i <- 0 to randomLength) {
          res += (keyList(i).asInstanceOf[T] -> valList(i).asInstanceOf[T])
        }
        return res.asInstanceOf[T]
      case s: StructType =>
        val array = s.fields.map(x => {
          generateWithSparkType(x.dataType)
        })
        Row.fromSeq(array)
      case x =>
        // TODO: Char, Varchar, Binary, Interval
        throw new NotImplementedError(s"data type $x not supported yet")
    }
    value.asInstanceOf[T]
  }
}
