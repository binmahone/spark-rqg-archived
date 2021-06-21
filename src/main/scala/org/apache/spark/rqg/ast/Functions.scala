package org.apache.spark.rqg.ast

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rqg._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.SparkSQLPrivateHelper

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
  // List of registered functions.
  private var registeredFunctions = ArrayBuffer[Function]()
  // List of functions that (a) were not registered manually and (b) we tried to register and
  // failed to do so. This should be empty after calling Init().
  private var failedFunctions = ArrayBuffer[String]()

  def getFunctions: Seq[Function] = {
    registerFunctionsImpl
    try {
      registeredFunctions
    } catch {
      case e: Throwable =>
        val msg = s"Unknown exception ${e.getClass.getName} - ${e.getMessage}"
        sys.error(msg)
    }
  }

  case object Generics {
    val A = GenericNamedType("A")
    val B = GenericNamedType("B")
    val C = GenericNamedType("C")
    val D = GenericNamedType("D")
  }

  // Make this a lazy value so we get a meaningful exception if it fails. If we do it on object initialization,
  // we get an an ambiguous ObjectInitializerException.
  lazy val registerFunctionsImpl: Set[Unit] =
    Set(
      // misc non-aggregate functions
      expression[Abs](FunctionType.MISCNONAGG),
      // Register coalesce() with up to three arguments.
      // expression[Coalesce](FunctionType.MISCNONAGG),
      registerFunction("coalesce", Generics.A, Seq(Generics.A)),
      registerFunction("coalesce", Generics.A, Seq(Generics.A, Generics.A)),
      registerFunction("coalesce", Generics.A, Seq(Generics.A, Generics.A, Generics.A)),
      // TODO(shoumik): Blacklisted because collection
      // expression[Explode](FunctionType.MISCNONAGG),
      // Register greatest() with up to three arguments. It takes at least two arguments.
      // expression[Greatest](FunctionType.MISCNONAGG),
      registerFunction("greatest", Generics.A, Seq(Generics.A, Generics.A)),
      registerFunction("greatest", Generics.A, Seq(Generics.A, Generics.A, Generics.A)),
      // expression[If](FunctionType.MISCNONAGG),
      registerFunction("if", Generics.A, Seq(BooleanType, Generics.A, Generics.A)),
      // TODO(shoumik): Blacklisted because collection
      // expression[Inline](FunctionType.MISCNONAGG),
      expression[IsNaN](FunctionType.MISCNONAGG),
      // expression[IfNull](FunctionType.MISCNONAGG),
      registerFunction("ifnull", Generics.A, Seq(Generics.A, Generics.A)),
      // expression[IsNull](FunctionType.MISCNONAGG),
      registerFunction("is_null", BooleanType, Seq(Generics.A)),
      // expression[IsNotNull](FunctionType.MISCNONAGG),
      // expression[Least](FunctionType.MISCNONAGG),
      registerFunction("least", Generics.A, Seq(Generics.A, Generics.A)),
      registerFunction("least", Generics.A, Seq(Generics.A, Generics.A, Generics.A)),
      expression[NaNvl](FunctionType.MISCNONAGG),
      // TODO(shoumik): Blacklisted because RuntimeReplaceable
      // expression[NullIf](FunctionType.MISCNONAGG),
      // TODO(shoumik): Blacklisted because RuntimeReplaceable
      // expression[Nvl](FunctionType.MISCNONAGG),
      // TODO(shoumik): Blacklisted because RuntimeReplaceable
      // expression[Nvl2](FunctionType.MISCNONAGG),
      // expression[PosExplode](FunctionType.MISCNONAGG),
      // TODO(shoumik): Disabled because they are nondeterministic.
      // expression[Rand](FunctionType.NONDETERMINISTIC),
      // expression[Randn](FunctionType.NONDETERMINISTIC),
      // expression[Stack](FunctionType.MISCNONAGG),
      // Register CaseWhen with up to 3 clauses.
      // expression[CaseWhen](FunctionType.MISCNONAGG),
      registerFunction("case_when", Generics.A, Seq(BooleanType, Generics.A, Generics.A)),
      registerFunction("case_when", Generics.A, Seq(
        BooleanType, Generics.A,
        BooleanType, Generics.A,
        Generics.A)),
      registerFunction("case_when", Generics.A, Seq(
        BooleanType, Generics.A,
        BooleanType, Generics.A,
        BooleanType, Generics.A,
        Generics.A)),
      // math functions
      expression[Acos](FunctionType.MATH),
      expression[Acosh](FunctionType.MATH),
      expression[Asin](FunctionType.MATH),
      expression[Asinh](FunctionType.MATH),
      expression[Atan](FunctionType.MATH),
      expression[Atan2](FunctionType.MATH),
      expression[Atanh](FunctionType.MATH),
      expression[Bin](FunctionType.MATH),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[BRound](FunctionType.MATH),
      expression[Cbrt](FunctionType.MATH),
      expression[Ceil](FunctionType.MATH),
      expression[Cos](FunctionType.MATH),
      expression[Cosh](FunctionType.MATH),
      expression[Conv](FunctionType.MATH),
      expression[ToDegrees](FunctionType.MATH),
      // expression[EulerNumber](FunctionType.MATH),
      expression[Exp](FunctionType.MATH),
      expression[Expm1](FunctionType.MATH),
      expression[Factorial](FunctionType.MATH),
      expression[Floor](FunctionType.MATH),
      expression[Hex](FunctionType.MATH),
      expression[Hypot](FunctionType.MATH),
      expression[Logarithm](FunctionType.MATH),
      expression[Log10](FunctionType.MATH),
      expression[Log1p](FunctionType.MATH),
      expression[Log2](FunctionType.MATH),
      expression[Log](FunctionType.MATH),
      // Covered by '%' operator.
      // expression[Remainder](FunctionType.MATH),
      // expression[UnaryMinus](FunctionType.MATH),
      // expression[Pi](FunctionType.MATH),
      expression[Pmod](FunctionType.MATH),
      expression[UnaryPositive](FunctionType.MATH),
      expression[Pow](FunctionType.MATH),
      expression[ToRadians](FunctionType.MATH),
      expression[Rint](FunctionType.MATH),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[Round](FunctionType.MATH),
      expression[ShiftLeft](FunctionType.MATH),
      expression[ShiftRight](FunctionType.MATH),
      expression[ShiftRightUnsigned](FunctionType.MATH),
      expression[Signum](FunctionType.MATH),
      expression[Sin](FunctionType.MATH),
      expression[Sinh](FunctionType.MATH),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[StringToMap](FunctionType.MATH),
      expression[Sqrt](FunctionType.MATH),
      expression[Tan](FunctionType.MATH),
      expression[Cot](FunctionType.MATH),
      expression[Tanh](FunctionType.MATH),
      // aggregate functions
      // expression[HyperLogLogPlusPlus](FunctionType.AGG),
      expression[Average](FunctionType.AGG),
      expression[Corr](FunctionType.AGG),
      // expression[Count](FunctionType.AGG), // not ExpectsInputTypes
      // Register count() for up to 3 arguments.
      registerFunction("count", BigIntType, Seq(Generics.A), isAgg = true),
      registerFunction("count", BigIntType, Seq(Generics.A, Generics.B),  isAgg = true),
      registerFunction("count", BigIntType, Seq(Generics.A, Generics.B, Generics.C), isAgg = true),
      expression[CountIf](FunctionType.AGG),
      expression[CovPopulation](FunctionType.AGG),
      expression[CovSample](FunctionType.AGG),
      // Disabled because it is non-deterministic.
      // expression[First](FunctionType.AGG),
      expression[Kurtosis](FunctionType.AGG),
      expression[Last](FunctionType.AGG),
      // expression[Max](FunctionType.AGG), // not ExpectsInputTypes
      registerFunction("max", Generics.A, Seq(Generics.A), isAgg = true),
      // expression[MaxBy](FunctionType.AGG),
      registerFunction("max_by", Generics.A, Seq(Generics.A, Generics.B), isAgg = true),
      // expression[Min](FunctionType.AGG), // not ExpectsInputTypes
      registerFunction("min", Generics.A, Seq(Generics.A), isAgg = true),
      // expression[MinBy](FunctionType.AGG),
      registerFunction("min_by", Generics.A, Seq(Generics.A, Generics.B), isAgg = true),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[Percentile](FunctionType.AGG),
      expression[Skewness](FunctionType.AGG),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[ApproximatePercentile](FunctionType.AGG),
      expression[StddevPop](FunctionType.AGG),
      expression[StddevSamp](FunctionType.AGG),
      expression[Sum](FunctionType.AGG),
      expression[VarianceSamp](FunctionType.AGG),
      expression[VariancePop](FunctionType.AGG),
      expression[VarianceSamp](FunctionType.AGG),
      expression[CollectList](FunctionType.AGG),
      expression[CollectSet](FunctionType.AGG),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[CountMinSketchAgg](FunctionType.AGG),
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
      // expression[FormatNumber](FunctionType.STRING),
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
      // expression[FormatString](FunctionType.STRING),
      // expression[RegExpExtract](FunctionType.STRING),
      // expression[RegExpReplace](FunctionType.STRING),
      // expression[StringRepeat](FunctionType.STRING),
      expression[StringReplace](FunctionType.STRING),
      expression[Overlay](FunctionType.STRING),
      expression[RLike](FunctionType.STRING),
      expression[StringRPad](FunctionType.STRING),
      expression[StringTrimRight](FunctionType.STRING),
      expression[Sentences](FunctionType.STRING),
      expression[SoundEx](FunctionType.STRING),
      // TODO(shoumik): Blacklisted because often causes OOM.
      // expression[StringSpace](FunctionType.STRING),
      expression[StringSplit](FunctionType.STRING),
      expression[Substring](FunctionType.STRING),
      expression[Substring](FunctionType.STRING),
      // TODO(shoumik): Blacklisted because RuntimeReplaceable
      // expression[Left](FunctionType.STRING),
      // TODO(shoumik): Blacklisted because RuntimeReplaceable
      // expression[Right](FunctionType.STRING),
      expression[SubstringIndex](FunctionType.STRING),
      expression[StringTranslate](FunctionType.STRING),
      expression[StringTrim](FunctionType.STRING),
      expression[Upper](FunctionType.STRING),
      // TODO(shoumik): Blacklisted because BINARY type is not supported.
      // expression[UnBase64](FunctionType.STRING),
      // TODO(shoumik): Blacklisted because BINARY type is not supported.
      // expression[Unhex](FunctionType.STRING),
      expression[Upper](FunctionType.STRING),
      // datetime functions
      expression[AddMonths](FunctionType.DATETIME),
      // TODO(shoumik): Disabled because they are nondeterministic.
      // expression[CurrentDate](FunctionType.DATETIME),
      // TODO(shoumik): Disabled because they are nondeterministic.
      // expression[CurrentTimestamp](FunctionType.DATETIME),
      expression[DateDiff](FunctionType.DATETIME),
      expression[DateAdd](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[DateFormatClass](FunctionType.DATETIME),
      expression[DateSub](FunctionType.DATETIME),
      expression[DayOfMonth](FunctionType.DATETIME),
      expression[DayOfYear](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[FromUnixTime](FunctionType.DATETIME),
      // 3.0.0 deprecated
      // expression[FromUTCTimestamp],
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[Hour](FunctionType.DATETIME),
      expression[LastDay](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[Minute](FunctionType.DATETIME),
      expression[Month](FunctionType.DATETIME),
      expression[MonthsBetween](FunctionType.DATETIME),
      expression[NextDay](FunctionType.DATETIME),
      expression[CurrentTimestamp](FunctionType.DATETIME),
      expression[Quarter](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[Second](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted for ???
      // expression[ParseToTimestamp](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted for ???
      // expression[ParseToDate](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[Second](FunctionType.DATETIME),
      // expression[ToUnixTimestamp](FunctionType.DATETIME),
      // 3.0.0 deprecated
      // expression[ToUTCTimestamp],
      expression[TruncDate](FunctionType.DATETIME),
      expression[TruncTimestamp](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because timezone needs to be set for resolution.
      // expression[UnixTimestamp](FunctionType.DATETIME),
      expression[DayOfWeek](FunctionType.DATETIME),
      expression[WeekDay](FunctionType.DATETIME),
      expression[WeekOfYear](FunctionType.DATETIME),
      expression[Year](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Breaks for some reason.
      // expression[TimeWindow](FunctionType.DATETIME),
      expression[MakeDate](FunctionType.DATETIME),
      expression[MakeTimestamp](FunctionType.DATETIME),
      // TODO(shoumik.palkar): Blacklisted because some arguments must be foldable.
      // expression[MakeInterval](FunctionType.DATETIME),
      // TODO(shoumik): Blacklisted because INTERVAL type is not supported.
      // expression[JustifyInterval](FunctionType.DATETIME),
      expression[DatePart](FunctionType.DATETIME),
      /*
      // collection functions
      expression[ArrayContains](FunctionType.COLLECTION),
      expression[ArraysOverlap](FunctionType.COLLECTION),
      expression[ArrayIntersect](FunctionType.COLLECTION),
      expression[ArrayJoin](FunctionType.COLLECTION),
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
      */
      // misc functions. Disabled because they are nondeterministic.
      /*
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
      */
    )

  /**
   * Resolves functions that have generic arguments into concrete arguments, returning a new
   * [[Function]] with only concrete arguments. If the function return type is generic, it and all
   * its matching types are set to `returnType`.
   */
  def resolveGenerics(
      function: Function,
      allowedTypes: Array[DataType[_]],
      returnType: DataType[_]): Function = {
    require(!returnType.isInstanceOf[GenericNamedType])
    val types = Seq(function.returnType) ++ function.inputTypes
    val genericTypes = types.filter(_.isInstanceOf[GenericNamedType]).toSet
    // Get random concrete types. If the function's return type was
    // generic, replace the random type assigned to the generic with the provided return type.
    val concreteTypes = genericTypes.zip(
      Seq.fill(genericTypes.size)(RandomUtils.generateRandomDataType(allowedTypes))).toMap ++
      Map((function.returnType, returnType))
    // Use the generic type -> concrete type mapping to assign each generic type a concrete type.
    val newTypes = types.map {
      case a: GenericNamedType => concreteTypes(a)
      case concreteType => concreteType
    }
    Function(function.name, newTypes(0), newTypes.drop(1), function.isAgg,
      function.nondeterministic)
  }

  /**
   * Registers the given function with the function registry.
   *
   * @param funcName         SQL name of the function, e.g., 'max' or 'array_intersect'
   * @param returnType
   * @param inputTypes       child expression return types, ordered from left to right. The ith
   *                         element is
   *                         the type of the ith input.
   * @param isAgg            whether this is an agregation.
   * @param nondeterministic whether this function returns non-deterministic results.
   */
  private def registerFunction(funcName: String,
    returnType: DataType[_],
    inputTypes: Seq[DataType[_]],
    isAgg: Boolean = false,
    nondeterministic: Boolean = false) {
    val function = Function(funcName, returnType, inputTypes, isAgg, nondeterministic)
    registeredFunctions += function
  }

  /**
   * Registers a Spark SQL expression represented by the class `T` by adding possible variants of
   * the function to `this.registeredFunctions`. The `functionType` determines
   * what kind of function this is, e.g., an aggregation function, collection function, etc.
   */
  private def expression[T <: Expression](
    functionType: FunctionType)(implicit tag: ClassTag[T]): Unit = {
    // Get the possible constructors for the expression type, and construct the expression using the
    // variant with the maximum number of arguments.
    var all = tag.runtimeClass.getConstructors
    val maxNumArgs = all.map(_.getParameterCount).max
    all = all.filter(_.getParameterCount == maxNumArgs)
    if (!classOf[ExpectsInputTypes].isAssignableFrom(tag.runtimeClass)) {
      val msg = (s"SKIPPING expression ${tag.runtimeClass} (not ExpectsInputTypes)")
      failedFunctions += msg
      return
    }

    all.take(1).foreach(c => {
      val paramTypes = c.getParameterTypes
      // Only handle expressions that check their input types here.
      // For the chosen constructor, create dummy arguments to pass to the constructor. The
      // Spark types of any expression arguments
      // are set to a placeholder, since we will only use the created instance to retrieve the
      // actual
      // expression argument types.
      val args = FunctionHelper.getArgs(paramTypes)
      // Create an instance so we can access the possible input types. Then, for each
      // combination of input types, create another instance so we can find the result type for
      // that combination of input types.
      val instance = c.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
      // Get the possible input types from the instance.
      val possibleInputTypes = instance.inputTypes
      // Iterate over all the possible input types.
      SparkSQLPrivateHelper.atomicInputTypesIterator(possibleInputTypes).foreach {
        case inputTypes =>
          val newArgs = FunctionHelper.getArgs(paramTypes, Some(inputTypes))
          val newInstance = c.newInstance(newArgs: _*).asInstanceOf[Expression]
          val returnType = newInstance.dataType
          // Try to resolve the expression type.
          val funcName = instance.prettyName
          // Check if this combination of input types works for this function. This also allows
          // us to find various constraints, e.g., that an expression argument needs to be
          // foldable.
          if (newInstance.resolved) {
            try {
              val isAgg = functionType == FunctionType.AGG
              val nondeterministic = functionType == FunctionType.NONDETERMINISTIC
              registerFunction(funcName, DataType(returnType), inputTypes.map(DataType(_)),
                isAgg, nondeterministic)
            } catch {
              // Ignore functions whose data types are not supported by the RQG.
              case e: IllegalArgumentException =>
                val msg = e.getCause()
                failedFunctions += s"SKIPPING function $funcName (invalid type) - $msg"
            }
          } else {
            val msg = s"SKIPPING function $funcName (could not resolve): message =" +
              s"${newInstance.checkInputDataTypes()}"
            failedFunctions += msg
          }
      }
    })
  }
}
