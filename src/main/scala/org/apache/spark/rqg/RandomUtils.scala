package org.apache.spark.rqg

import scala.util.Random

import org.apache.spark.internal.Logging

object RandomUtils extends Logging {

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
      logWarning(s"RandomUtils is already inited. new seed: $randomSeed doesn't take effect")
    }
  }

  def getSeed: Int = randomSeed

  def nextChoice[T](choices: Array[T]): T = {
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

  def nextBoolean(probability: Double): Boolean = getRandom.nextDouble() >= probability

  def nextConstant[T](dataType: DataType[T]): T = getValueGenerator.generateValue(dataType)
}

trait WeightedChoice {
  def weightName: String
}
