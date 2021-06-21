package org.apache.spark.rqg

import java.sql.{Date, Timestamp}
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.{Row, types => sparktypes}

class ValueGenerator(random: Random) {

  // Random seed time. Represents ~3:10PM PDT on 12/08/20.
  val SEED_EPOCH_TIME = 1607469075134L

  def getRandomEpochTime(): Long = {
    val randomVal = random.nextDouble()
    (randomVal * SEED_EPOCH_TIME).toLong
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
        // Get the number of bits in the biggest number possible for this precision.
        val maxNumBits = new java.math.BigInteger("9" * d.precision).bitLength()
        // Create a random decimal with one less than the maximum number of bits.
        val unscaledValue = new java.math.BigInteger(scala.math.max(1, maxNumBits - 1), random.self)
        val res = BigDecimal(
          new java.math.BigDecimal(unscaledValue, d.scale, new java.math.MathContext(d.precision)))
        res
      case a: ArrayType =>
        val randomLength = random.nextInt(5) + 1
        (0 to randomLength).toArray.map(_=> generateValue(a.innerType))
      case m: MapType =>
        val randomLength = random.nextInt(5) + 1
        // generate two lists of key and value based the their types
        val keyList = (0 to randomLength).toArray.map(_ => generateValue(m.keyType))
        val valList = (0 to randomLength).toArray.map(_ => generateValue(m.valueType))
        val res = mutable.Map[T, T]()
        for (i <- 0 to randomLength) {
          res += (keyList(i).asInstanceOf[T] -> valList(i).asInstanceOf[T])
        }
        return res.asInstanceOf[T]
      case s: StructType =>
        val array = s.fields.map(field => generateValue(field.dataType))
        Row.fromSeq(array)
      case x =>
        // TODO: Char, Varchar, Binary, Interval
        throw new NotImplementedError(s"data type $x not supported yet")
    }
    value.asInstanceOf[T]
  }
}
