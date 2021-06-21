package org.apache.spark.rqg

import scala.util.Random
import org.apache.spark.internal.Logging

final case class RQGEmptyChoiceException(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object RandomUtils extends Logging {
  private val rqgConfig: RQGConfig = RQGConfig.load()
  private var randomSeed: Int = new Random().nextInt()
  private var random: Random = _
  private var valueGenerator: ValueGenerator = _

  private def init(): Unit = synchronized {
    if (random == null) {
      random = new Random(randomSeed)
      valueGenerator = new ValueGenerator(random)
      logDebug(s"RandomUtils inited with randomSeed = $randomSeed")
    }
  }

  def getRandom: Random = {
    if (random == null) init()
    random
  }

  def getValueGenerator: ValueGenerator = {
    if (valueGenerator == null) init()
    valueGenerator
  }

  def setSeed(randomSeed: Int, reset: Boolean = false): Unit = {
    if (random == null || reset) {
      this.randomSeed = randomSeed
      this.random = null
      this.valueGenerator = null
    } else {
      logDebug(s"RandomUtils is already inited. new seed: $randomSeed doesn't take effect")
    }
  }

  def getSeed: Int = randomSeed

  def nextChoice[T](choices: Array[T]): T = {
    if (choices.isEmpty) {
      throw RQGEmptyChoiceException("No choices left while trying to generate expression")
    }
    choices(getRandom.nextInt(choices.length))
  }

  def choice[T <: WeightedChoice](choices: Array[T], weights: List[WeightEntry]): T = {
    // First find entry exists both in choices and weights
    val filteredWeights =
      weights.filter(w => choices.exists(_.weightName.toLowerCase == w.key.toLowerCase))
    val totalWeight = filteredWeights.map(_.value).sum
    var predicate = getRandom.nextDouble() * totalWeight
    for (entry <- filteredWeights) {
      if (entry.value > 0 && entry.value > predicate) {
        return choices.find(_.weightName.toLowerCase == entry.key.toLowerCase).get
      }
      predicate -= entry.value
    }
    throw new IllegalArgumentException("Couldn't choose a weighted choice. " +
      s"choices: [${choices.map(_.weightName).mkString(", ")}], " +
      s"config weights: [${weights.mkString(",")}]")
  }

  def choice(inclusiveStart: Int, inclusiveEnd: Int): Int = {
    require(inclusiveEnd >= inclusiveStart)
    getRandom.nextInt(inclusiveEnd - inclusiveStart + 1) + inclusiveStart
  }

  def nextInt(n: Int): Int = getRandom.nextInt(n)

  def nextBoolean(): Boolean = getRandom.nextBoolean()

  def nextBoolean(probability: Double): Boolean = getRandom.nextDouble() <= probability

  def nextValue[T](dataType: DataType[T]): T =
    if (getRandom.nextDouble() > rqgConfig.getProbability(RQGConfig.DATA_GENERATOR_NULL)) {
      getValueGenerator.generateValue(dataType)
    } else {
      null.asInstanceOf[T]
    }

  def nextConstant[T](dataType: DataType[T]): T =
    getValueGenerator.generateValue(dataType)

  def getNotOrEmpty(): String = {
    if (nextBoolean()) {
      "NOT"
    } else {
      ""
    }
  }

  /**
   * Generates a random data type, selecting types from the list of allowed types. If
   * `nestedCount >= 1`, the data type may be a struct, array, or map: each nested type is also
   * selected from `allowedTypes`. If `nestedCount` is None, the `RQGConfig` value is used.
   *
   * `allowedDataTypes` treats nested and parameterized types as "generic", i.e., if
   * `allowedDataTypes` contains a STRUCT, the struct fields will be generated randomly. Use
   * `choice()` if you want to pick an exact data type from the list of allowed types.
   */
  def generateRandomDataType(
      allowedTypes: Array[DataType[_]],
      maxNestingDepthOpt: Option[Int] = None,
      weightsOpt: Option[List[WeightEntry]] = None): DataType[_] = {
    // Find the max allowed nesting depth.
    val (_, confMaxNestingDepth) =
      rqgConfig.getBound(RQGConfig.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT)
    val maxNestingDepth = maxNestingDepthOpt.getOrElse(confMaxNestingDepth)

    // If the nesting depth is 0, we must pick a non-nested type.
    val filteredChoices = allowedTypes.filter {
      case _: StructType | _: ArrayType | _: MapType if maxNestingDepth <= 0 => false
      case _ => true
    }

    val typeToGenerate = weightsOpt match {
      case Some(weights) => this.choice(filteredChoices, weights)
      case None => this.nextChoice(filteredChoices)
    }

    typeToGenerate match {
      case _: StructType =>
        val numStructFields = 1 + nextInt(2)
        // TODO(shoumik): No decimal because it complicates the parsing of the schema string..
        val allowedTypesWithoutDecimal = allowedTypes.filterNot(_.isInstanceOf[DecimalType])
        val fields = (0 until numStructFields).map { fieldIdx =>
          val generated = generateRandomDataType(
            allowedTypesWithoutDecimal,
            Some(maxNestingDepth - 1),
            weightsOpt)
          StructField(s"${generated.fieldName}$fieldIdx", generated)
        }.toArray
        StructType(fields)
      case _: MapType =>
        MapType(
          // Map keys are not allowed to be maps in Spark.
          generateRandomDataType(
            allowedTypes.filter(_.isInstanceOf[MapType]),
            Some(maxNestingDepth - 1),
            weightsOpt),
          generateRandomDataType(
            allowedTypes,
            Some(maxNestingDepth - 1),
            weightsOpt)
        )
      case _: ArrayType =>
        ArrayType(
          generateRandomDataType(allowedTypes, Some(maxNestingDepth - 1), weightsOpt)
        )
      case _: DecimalType =>
        val precision = RandomUtils.choice(1, DecimalType.MAX_PRECISION)
        val scale = RandomUtils.choice(0, precision)
        DecimalType(precision, scale)
      case other => other
    }
  }
}

trait WeightedChoice {
  def weightName: String
}
