package com.baidu.spark.rqg

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

  def choice(inclusiveStart: Int, inclusiveEnd: Int): Int = {
    require(inclusiveEnd >= inclusiveStart)
    getRandom.nextInt(inclusiveEnd - inclusiveStart + 1) + inclusiveStart
  }

  def nextInt(n: Int): Int = getRandom.nextInt(n)

  def nextBoolean(): Boolean = getRandom.nextBoolean()

  def nextConstant[T](dataType: DataType[T]): T = getValueGenerator.generateValue(dataType)
}
