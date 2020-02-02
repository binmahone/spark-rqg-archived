package com.baidu.spark.rqg.new_ast

import scala.util.Random

import com.baidu.spark.rqg.{DataType, ValueGenerator}

import org.apache.spark.internal.Logging

object RandomUtils extends Logging {

  // TODO: get random seed from config or system property
  private val randomSeed = new Random().nextInt()
  logInfo(s"RandomUtils inited with randomSeed = $randomSeed")

  private val random = new Random(randomSeed)

  private val valueGenerator = new ValueGenerator(random)

  def choice[T](choices: Array[T]): T = {
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
