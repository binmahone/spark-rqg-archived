package org.apache.spark.rqg.ast


import java.lang.reflect.Constructor

import org.apache.spark.rqg._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.SparkSQLPrivateHelper
import org.apache.spark.sql.{types => sparktypes}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.reflect.ClassTag

sealed trait FunctionType

object FunctionType {
  case object AGG extends FunctionType
  case object STRING extends FunctionType
  case object MISCNONAGG extends FunctionType
  case object NONDETERMINISTIC extends FunctionType
  case object MATH extends FunctionType
  case object DATETIME extends FunctionType
  case object COLLECTION extends FunctionType
  case object MISC extends FunctionType
}

object Functions {
  var registeredFunctions = ArrayBuffer[Function]()
  var success = 0
  var failed = 0

  val dataTypes: Array[sparktypes.DataType] = Array(
    sparktypes.BinaryType,
    sparktypes.BooleanType,
    sparktypes.ByteType,
    sparktypes.CalendarIntervalType,
    sparktypes.DateType,
    sparktypes.DoubleType,
    sparktypes.FloatType,
    sparktypes.IntegerType,
    sparktypes.LongType,
    sparktypes.NullType,
    sparktypes.ShortType,
    sparktypes.StringType,
    sparktypes.TimestampType,
  )

  val integral = Class.forName("org.apache.spark.sql.types.IntegralType")
  val numeric = Class.forName("org.apache.spark.sql.types.NumericType")
  val numericDataTypes = DataType.supportedDataTypes.filter(t => numeric.isAssignableFrom(t.sparkType.getClass))
  val integralDataTypes = DataType.supportedDataTypes.filter(t => integral.isAssignableFrom(t.sparkType.getClass))

  val functions = Seq(
    // misc non-aggregate functions
    expression[Abs](FunctionType.MISCNONAGG),
    expression[Coalesce](FunctionType.MISCNONAGG),
    expression[Explode](FunctionType.MISCNONAGG),
    expression[Greatest](FunctionType.MISCNONAGG),
    expression[If](FunctionType.MISCNONAGG),
    expression[Inline](FunctionType.MISCNONAGG),
    expression[IsNaN](FunctionType.MISCNONAGG),
    expression[IfNull](FunctionType.MISCNONAGG),
    expression[IsNull](FunctionType.MISCNONAGG),
    expression[IsNotNull](FunctionType.MISCNONAGG),
    expression[Least](FunctionType.MISCNONAGG),
    expression[NaNvl](FunctionType.MISCNONAGG),
    expression[NullIf](FunctionType.MISCNONAGG),
    expression[Nvl](FunctionType.MISCNONAGG),
    expression[Nvl2](FunctionType.MISCNONAGG),
    expression[PosExplode](FunctionType.MISCNONAGG),
//    expression[Rand](FunctionType.NONDETERMINISTIC),
//    expression[Randn](FunctionType.NONDETERMINISTIC),
    expression[Stack](FunctionType.MISCNONAGG),
    expression[CaseWhen](FunctionType.MISCNONAGG),
    // math functions
    expression[Acos](FunctionType.MATH),
    expression[Acosh](FunctionType.MATH),
    expression[Asin](FunctionType.MATH),
    expression[Asinh](FunctionType.MATH),
    expression[Atan](FunctionType.MATH),
    expression[Atan2](FunctionType.MATH),
    expression[Atanh](FunctionType.MATH),
    expression[Bin](FunctionType.MATH),
    expression[BRound](FunctionType.MATH),
    expression[Cbrt](FunctionType.MATH),
    expression[Cos](FunctionType.MATH),
    expression[Cosh](FunctionType.MATH),
    expression[Conv](FunctionType.MATH),
    expression[ToDegrees](FunctionType.MATH),
    expression[EulerNumber](FunctionType.MATH),
    expression[Exp](FunctionType.MATH),
    expression[Expm1](FunctionType.MATH),
    expression[Factorial](FunctionType.MATH),
    expression[Hex](FunctionType.MATH),
    expression[Hypot](FunctionType.MATH),
    expression[Logarithm](FunctionType.MATH),
    expression[Log10](FunctionType.MATH),
    expression[Log1p](FunctionType.MATH),
    expression[Log2](FunctionType.MATH),
    expression[Log](FunctionType.MATH),
    expression[Remainder](FunctionType.MATH),
//    expression[UnaryMinus](FunctionType.MATH),
    expression[Pi](FunctionType.MATH),
    expression[Pmod](FunctionType.MATH),
    expression[UnaryPositive](FunctionType.MATH),
    expression[Pow](FunctionType.MATH),
    expression[ToRadians](FunctionType.MATH),
    expression[Rint](FunctionType.MATH),
    expression[Round](FunctionType.MATH),
    expression[ShiftLeft](FunctionType.MATH),
    expression[ShiftRight](FunctionType.MATH),
    expression[ShiftRightUnsigned](FunctionType.MATH),
    expression[Signum](FunctionType.MATH),
    expression[Sin](FunctionType.MATH),
    expression[Sinh](FunctionType.MATH),
    expression[StringToMap](FunctionType.MATH),
    expression[Sqrt](FunctionType.MATH),
    expression[Tan](FunctionType.MATH),
    expression[Cot](FunctionType.MATH),
    expression[Tanh](FunctionType.MATH),
    // aggregate functions
    expression[HyperLogLogPlusPlus](FunctionType.AGG),
    expression[Average](FunctionType.AGG),
    expression[Corr](FunctionType.AGG),
    expression[Count](FunctionType.AGG),
    expression[CountIf](FunctionType.AGG),
    expression[CovPopulation](FunctionType.AGG),
    expression[CovSample](FunctionType.AGG),
    expression[First](FunctionType.AGG),
    expression[Kurtosis](FunctionType.AGG),
    expression[Last](FunctionType.AGG),
    expression[Max](FunctionType.AGG),
    expression[MaxBy](FunctionType.AGG),
    expression[Min](FunctionType.AGG),
    expression[MinBy](FunctionType.AGG),
    expression[Percentile](FunctionType.AGG),
    expression[Skewness](FunctionType.AGG),
    expression[ApproximatePercentile](FunctionType.AGG),
    expression[StddevPop](FunctionType.AGG),
    expression[StddevSamp](FunctionType.AGG),
    expression[Sum](FunctionType.AGG),
    expression[VarianceSamp](FunctionType.AGG),
    expression[VariancePop](FunctionType.AGG),
    expression[VarianceSamp](FunctionType.AGG),
    expression[CollectList](FunctionType.AGG),
    expression[CollectSet](FunctionType.AGG),
    expression[CountMinSketchAgg](FunctionType.AGG),
    // string functions
    expression[Ascii](FunctionType.STRING),
    expression[Chr](FunctionType.STRING),
    expression[Chr](FunctionType.STRING),
    expression[Base64](FunctionType.STRING),
    expression[BitLength](FunctionType.STRING),
    expression[Length](FunctionType.STRING),
    expression[Length](FunctionType.STRING),
    expression[ConcatWs](FunctionType.STRING),
    expression[Decode](FunctionType.STRING),
    expression[Elt](FunctionType.STRING),
    expression[Encode](FunctionType.STRING),
    expression[FindInSet](FunctionType.STRING),
    expression[FormatNumber](FunctionType.STRING),
    expression[FormatString](FunctionType.STRING),
    expression[GetJsonObject](FunctionType.STRING),
    expression[InitCap](FunctionType.STRING),
    expression[StringInstr](FunctionType.STRING),
    expression[Lower](FunctionType.STRING),
    expression[Length](FunctionType.STRING),
    expression[Levenshtein](FunctionType.STRING),
    expression[Lower](FunctionType.STRING),
    expression[OctetLength](FunctionType.STRING),
    expression[StringLocate](FunctionType.STRING),
    expression[StringLPad](FunctionType.STRING),
    expression[StringTrimLeft](FunctionType.STRING),
    expression[StringLocate](FunctionType.STRING),
    expression[FormatString](FunctionType.STRING),
    expression[RegExpExtract](FunctionType.STRING),
    expression[RegExpReplace](FunctionType.STRING),
    expression[StringRepeat](FunctionType.STRING),
    expression[StringReplace](FunctionType.STRING),
    expression[Overlay](FunctionType.STRING),
    expression[RLike](FunctionType.STRING),
    expression[StringRPad](FunctionType.STRING),
    expression[StringTrimRight](FunctionType.STRING),
    expression[Sentences](FunctionType.STRING),
    expression[SoundEx](FunctionType.STRING),
    expression[StringSpace](FunctionType.STRING),
    expression[StringSplit](FunctionType.STRING),
    expression[Substring](FunctionType.STRING),
    expression[Substring](FunctionType.STRING),
    expression[Left](FunctionType.STRING),
    expression[Right](FunctionType.STRING),
    expression[SubstringIndex](FunctionType.STRING),
    expression[StringTranslate](FunctionType.STRING),
    expression[StringTrim](FunctionType.STRING),
    expression[Upper](FunctionType.STRING),
    expression[UnBase64](FunctionType.STRING),
    expression[Unhex](FunctionType.STRING),
    expression[Upper](FunctionType.STRING),
//    // datetime functions
    expression[AddMonths](FunctionType.DATETIME),
    expression[CurrentDate](FunctionType.DATETIME),
    expression[CurrentTimestamp](FunctionType.DATETIME),
    expression[DateDiff](FunctionType.DATETIME),
    expression[DateAdd](FunctionType.DATETIME),
    expression[DateFormatClass](FunctionType.DATETIME),
    expression[DateSub](FunctionType.DATETIME),
    expression[DayOfMonth](FunctionType.DATETIME),
    expression[DayOfYear](FunctionType.DATETIME),
    expression[DayOfMonth](FunctionType.DATETIME),
    expression[FromUnixTime](FunctionType.DATETIME),
    // 3.0.0 deprecated
    //FromUTCTimestamp],
    expression[Hour](FunctionType.DATETIME),
    expression[LastDay](FunctionType.DATETIME),
    expression[Minute](FunctionType.DATETIME),
    expression[Month](FunctionType.DATETIME),
    expression[MonthsBetween](FunctionType.DATETIME),
    expression[NextDay](FunctionType.DATETIME),
    expression[CurrentTimestamp](FunctionType.DATETIME),
    expression[Quarter](FunctionType.DATETIME),
    expression[Second](FunctionType.DATETIME),
    expression[ParseToTimestamp](FunctionType.DATETIME),
    expression[ParseToDate](FunctionType.DATETIME),
    expression[ToUnixTimestamp](FunctionType.DATETIME),
    // 3.0.0 deprecated
    //ToUTCTimestamp],
    expression[TruncDate](FunctionType.DATETIME),
    expression[TruncTimestamp](FunctionType.DATETIME),
    expression[UnixTimestamp](FunctionType.DATETIME),
    expression[DayOfWeek](FunctionType.DATETIME),
    expression[WeekDay](FunctionType.DATETIME),
    expression[WeekOfYear](FunctionType.DATETIME),
    expression[Year](FunctionType.DATETIME),
    expression[TimeWindow](FunctionType.DATETIME),
    expression[MakeDate](FunctionType.DATETIME),
    expression[MakeTimestamp](FunctionType.DATETIME),
    expression[MakeInterval](FunctionType.DATETIME),
    expression[JustifyDays](FunctionType.DATETIME),
    expression[JustifyHours](FunctionType.DATETIME),
    expression[JustifyInterval](FunctionType.DATETIME),
    expression[DatePart](FunctionType.DATETIME),
//    // collection functions
    expression[ArrayContains](FunctionType.COLLECTION),
//    expression[ArraysOverlap](FunctionType.COLLECTION),
    expression[ArrayIntersect](FunctionType.COLLECTION),
//    expression[ArrayJoin](FunctionType.COLLECTION),
    expression[ArrayPosition](FunctionType.COLLECTION),
    expression[ArraySort](FunctionType.COLLECTION),
    expression[ArrayExcept](FunctionType.COLLECTION),
    expression[ArrayUnion](FunctionType.COLLECTION),
    expression[ElementAt](FunctionType.COLLECTION),
    expression[MapFromArrays](FunctionType.COLLECTION),
    expression[MapKeys](FunctionType.COLLECTION),
    expression[MapValues](FunctionType.COLLECTION),
    expression[MapEntries](FunctionType.COLLECTION),
    expression[MapFromEntries](FunctionType.COLLECTION),
    expression[MapConcat](FunctionType.COLLECTION),
    expression[Size](FunctionType.COLLECTION),
    expression[Slice](FunctionType.COLLECTION),
    expression[Size](FunctionType.COLLECTION),
    expression[ArraysZip](FunctionType.COLLECTION),
    expression[SortArray](FunctionType.COLLECTION),
    expression[Shuffle](FunctionType.COLLECTION),
    expression[ArrayMin](FunctionType.COLLECTION),
    expression[ArrayMax](FunctionType.COLLECTION),
    expression[Reverse](FunctionType.COLLECTION),
    expression[Concat](FunctionType.COLLECTION),
    expression[Flatten](FunctionType.COLLECTION),
    expression[Sequence](FunctionType.COLLECTION),
    expression[ArrayRepeat](FunctionType.COLLECTION),
    expression[ArrayRemove](FunctionType.COLLECTION),
    expression[ArrayDistinct](FunctionType.COLLECTION),
    expression[ArrayTransform](FunctionType.COLLECTION),
    expression[MapFilter](FunctionType.COLLECTION),
    expression[ArrayFilter](FunctionType.COLLECTION),
    expression[ArrayExists](FunctionType.COLLECTION),
    expression[ArrayForAll](FunctionType.COLLECTION),
    expression[ArrayAggregate](FunctionType.COLLECTION),
    expression[TransformValues](FunctionType.COLLECTION),
    expression[TransformKeys](FunctionType.COLLECTION),
    expression[MapZipWith](FunctionType.COLLECTION),
    expression[ZipWith](FunctionType.COLLECTION),
//    // misc functions
    expression[AssertTrue](FunctionType.MISC),
    expression[Crc32](FunctionType.MISC),
    expression[Md5](FunctionType.MISC),
    expression[Uuid](FunctionType.MISC),
    expression[Murmur3Hash](FunctionType.MISC),
    expression[XxHash64](FunctionType.MISC),
    expression[Sha1](FunctionType.MISC),
    expression[Sha2](FunctionType.MISC),
    expression[SparkPartitionID](FunctionType.MISC),
    expression[InputFileName](FunctionType.MISC),
    expression[InputFileBlockStart](FunctionType.MISC),
    expression[InputFileBlockLength](FunctionType.MISC),
    expression[MonotonicallyIncreasingID](FunctionType.MISC),
    expression[CurrentDatabase](FunctionType.MISC),
    expression[CallMethodViaReflection](FunctionType.MISC),
    expression[CallMethodViaReflection](FunctionType.MISC),
    expression[SparkVersion](FunctionType.MISC),
    expression[TypeOf](FunctionType.MISC),
  )

  def registerFunctions(c: Constructor[_], funcName: String, inputTypes: Seq[_], returnType: DataType[_], functionType: FunctionType): Any = {
    val inputs: Seq[DataType[_]] = inputTypes.map(inputType => {
      val typeString = inputType.toString
      if (typeString.contains("Numeric")) {
        RandomUtils.nextChoice(numericDataTypes)
      } else if (typeString.contains("IntegralType")) {
        RandomUtils.nextChoice(integralDataTypes)
      } else if (typeString.contains("AnyDataType")) {
        RandomUtils.nextChoice(DataType.supportedDataTypes)
      } else {
        RandomUtils.nextChoice(DataType.supportedDataTypes.filter(dt => dt.sparkType.getClass.isAssignableFrom(inputType.getClass)))
      }
    })

    val f = Function(funcName, returnType, inputs, isAgg = functionType == FunctionType.AGG, nondeterministic = functionType == FunctionType.NONDETERMINISTIC)
    registeredFunctions += f
  }

   def registerCustomFunction(funcName: String,
                             returnType: DataType[_],
                             inputTypes: Seq[DataType[_]],
                             isAgg: Boolean,
                             nondeterministic: Boolean) {
    val function = Function(funcName, returnType, inputTypes, isAgg, nondeterministic)
    registeredFunctions += function
  }

  def expression[T <: Expression](functionType: FunctionType)(implicit tag: ClassTag[T]) = {
    var all = tag.runtimeClass.getConstructors
//    val maxNumArgs = all.map(_.getParameterCount).max
//    all = all.filter(_.getParameterCount == maxNumArgs)
    all.foreach(c => {
      val paramTypes = c.getParameterTypes
      if (classOf[ExpectsInputTypes].isAssignableFrom(tag.runtimeClass)) {
        val chosen = RandomUtils.nextChoice(DataType.supportedDataTypes)
        val args = FunctionHelper.getArgs(paramTypes, chosen.sparkType)
        try {
          val instance = c.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
          val returnType = instance.dataType
          val potentialMatches = DataType.supportedDataTypes.filter(dt => dt.sparkType.sameType(returnType))
          val chosenReturnType = RandomUtils.nextChoice(potentialMatches)
          val funcName = instance.prettyName
          val typeCollection = Class.forName("org.apache.spark.sql.types.TypeCollection")
          val inputTypes = instance.inputTypes.map(it => {
            if (it.getClass.isAssignableFrom(typeCollection)) {
              SparkSQLPrivateHelper.getHelp(it, dataTypes, RandomUtils.getRandom)
            } else {
              it
            }
          })
          if (inputTypes.nonEmpty) {
            registerFunctions(c, funcName, inputTypes, chosenReturnType, functionType)
            success += 1
          }
        } catch {
          case e: Throwable => {
            failed += 1
          }
        }
      }
    })
  }

  // Register your custom functions here
  registerCustomFunction("floor", BigIntType, Seq(DecimalType()), isAgg = false, nondeterministic = false)
}
