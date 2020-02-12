package com.baidu.spark.rqg

import scala.util.Random

import org.apache.spark.internal.Logging

object RandomUtils extends Logging {

  // TODO: get random seed from config or system property
  private val randomSeed = new Random().nextInt()
  logInfo(s"RandomUtils inited with randomSeed = $randomSeed")

  private val random = new Random(900222757)

  private val valueGenerator = new ValueGenerator(random)

  def nextChoice[T](choices: Array[T]): T = {
    choices(random.nextInt(choices.length))
  }

  def choice(inclusiveStart: Int, inclusiveEnd: Int): Int = {
    require(inclusiveEnd >= inclusiveStart)
    random.nextInt(inclusiveEnd - inclusiveStart + 1) + inclusiveStart
  }

  def nextInt(n: Int): Int = random.nextInt(n)

  def nextBoolean(): Boolean = random.nextBoolean()

  def nextConstant[T](dataType: DataType[T]): T = valueGenerator.generateValue(dataType)
}
