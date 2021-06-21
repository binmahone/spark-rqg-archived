package org.apache.spark.sql.types

import scala.util.Random

object SparkSQLPrivateHelper {
  def getHelp(collectionType: Any, dataTypes: Seq[DataType], random: Random): DataType = {
    val instance = collectionType.asInstanceOf[TypeCollection]
    val accepts = dataTypes.filter(dt => instance.acceptsType(dt))
    accepts(random.nextInt(accepts.size))
  }

  private def integralTypes: Seq[DataType] = Seq(ShortType, IntegerType, LongType)
  // TODO(shoumik.palkar): Decimal?
  private def fractionalTypes: Seq[DataType] = Seq(FloatType, DoubleType)
  private def numericTypes: Seq[DataType] = integralTypes ++ fractionalTypes
  private def supportedAtomicTypes: Seq[DataType] =
    integralTypes ++ fractionalTypes ++ Seq(TimestampType, DateType)

  // From https://stackoverflow.com/questions/19768545/generalize-list-combinations-to-n-lists
  def combinationList[T](ls: Seq[Seq[T]]): List[List[T]] = ls match {
    case Nil => Nil :: Nil
    case head :: tail => val rec = combinationList[T](tail)
      rec.flatMap(r => head.map(t => t :: r))
  }

  /**
   * Given a set if possible input types for an expression (where the ith type is the possible input
   * type for the ith child expression), returns an iterator over all possible atomic types that
   * can be passed into the expression. Note that this does not cover nested types.
   */
  def atomicInputTypesIterator(possibleInputTypes: Seq[AbstractDataType]): List[List[DataType]] = {
    def getConcreteTypes(inputs: Seq[AbstractDataType]): Seq[Seq[DataType]] = inputs.map {
      case ty if supportedAtomicTypes.contains(ty) => Seq(ty.asInstanceOf[DataType])
      case StringType => Seq(StringType)
      case BinaryType => Seq(BinaryType)
      case IntegralType => integralTypes
      case _: FractionalType => fractionalTypes
      case NumericType => numericTypes
      case ty if ty == AnyDataType => supportedAtomicTypes
      case TypeCollection(possibleTypes) => getConcreteTypes(possibleTypes).flatten
      case _ => Seq()
    }
    val candidatesForEachChild = getConcreteTypes(possibleInputTypes)
    combinationList(candidatesForEachChild)
  }
}
