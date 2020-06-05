package org.apache.spark.rqg

import scala.util.Random
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{types => sparktypes}

final case class RQGEmptyChoiceException(
    private val message: String = "",
    private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

object RandomUtils extends Logging {

  private var rqgConfig: RQGConfig = RQGConfig.load()
  private var randomSeed: Int = new Random().nextInt()
  private var random: Random = _
  private var valueGenerator: ValueGenerator = _

  private def init(): Unit = synchronized {
    if (random == null) {
      random = new Random(randomSeed)
      valueGenerator = new ValueGenerator(random)
      logInfo(s"RandomUtils inited with randomSeed = $randomSeed")
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
      logInfo(s"RandomUtils is already inited. new seed: $randomSeed doesn't take effect")
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

  def generateRandomStructSchema(nestedCountOfStruct: Int): sparktypes.DataType = {
    // This count how many time the element inside a struct can be nested.
    // The element cannot be struct
    val randomNestedInFields = RandomUtils.choice(0, 2)
    sparktypes.StructType(generateRandomStructFields(nestedCountOfStruct, randomNestedInFields))
  }

  /**
   * Generate random struct fields. A field can be another struct, array, map, or primitive types
   * @param nestedCountOfStruct this count how many time a struct can be nested
   * @param nestedCountOfFields this count how many time the element inside a struct can be nested.
   *                            The element cannot be struct
   * @return an Array of struct fields
   */
  def generateRandomStructFields(nestedCountOfStruct: Int, nestedCountOfFields: Int): Array[sparktypes.StructField] = {
    val length = getRandom.nextInt(2) + 1
    (0 to length).toArray.zipWithIndex.map( {
      case (_, index) =>
        // if we randomly should generate spark datatype (other than struct)
        // or struct cannot be nested anymore
        if (getRandom.nextBoolean() || nestedCountOfStruct == 0) {
          val generated = generateRandomSparkDataType(nestedCountOfFields)
          StructField(generated.typeName + index, generated)
        } else {
          // Struct can be nested
          val generatedStruct = generateRandomStructSchema(nestedCountOfStruct - 1)
          StructField(generatedStruct.typeName + index, generatedStruct)
        }
    })
  }

  def generateRandomArray(nestedCount: Int): sparktypes.DataType = {
    sparktypes.ArrayType(generateRandomSparkDataType(nestedCount - 1))
  }

  def generateRandomMap(nestedCount: Int): sparktypes.DataType = {
    sparktypes.MapType(
      generateRandomSparkDataType(nestedCount - 1),
      generateRandomSparkDataType(nestedCount - 1))
  }

  /**
   * Generate randomly nested spark data type
   * @param nestedCount number of times a datatype can be nested
   *                    Ex:
   *                    Array<Int> = 0
   *                    Array<Array<Int>> = 1
   *                    Array<Array<Int>>>
   *
   * @return a randomly nested or non-nested spark data type
   */
  def generateRandomSparkDataType(nestedCount: Int = 1): sparktypes.DataType = {
    if (nestedCount == 0 || getRandom.nextBoolean()) {
      nextChoice(DataType.primitiveSparkDataTypes)
    } else if (nestedCount > 0) {
      val choice = RandomUtils.choice(0, 1)
        if (choice == 0) {
          generateRandomArray(nestedCount)
        } else {
          generateRandomMap(nestedCount)
        }
    } else {
      nextChoice(DataType.primitiveSparkDataTypes)
    }
  }
}

trait WeightedChoice {
  def weightName: String
}
