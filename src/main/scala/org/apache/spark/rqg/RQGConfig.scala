package org.apache.spark.rqg

import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}

class RQGConfig(config: Config) {
  def getBound(entry: RQGConfigEntry): (Int, Int) = {
    if (config.hasPath(entry.key)) {
      val v = config.getIntList(entry.key)
      (v.get(0), v.get(1))
    } else {
      entry.defaultValue.asInstanceOf[(Int, Int)]
    }
  }

  def getProbability(entry: RQGConfigEntry): Double = {
    if (config.hasPath(entry.key)) {
      config.getDouble(entry.key)
    } else {
      entry.defaultValue.asInstanceOf[Double]
    }
  }

  def getWeight(entry: RQGConfigEntry): List[WeightEntry] = {
    if (config.hasPath(entry.key)) {
      config.getConfig(entry.key).entrySet().asScala.map { v =>
        WeightEntry(v.getKey, config.getDouble(entry.key + "." + v.getKey))
      }.toList
    } else {
      entry.defaultValue.asInstanceOf[List[WeightEntry]]
    }
  }

  private def getSparkConfig(key: String): Map[String, String] = {
    require(key == RQGConfig.REFERENCE_SPARK_CONF || key == RQGConfig.TEST_SPARK_CONF ||
      key == RQGConfig.COMMON_SPARK_CONF)
    if (config.hasPath(key)) {
      config.getConfig(key).entrySet().asScala.map { v =>
        v.getKey -> config.getString(s"$key.${v.getKey}")
      }.toMap
    } else {
      Map.empty
    }
  }

  def getRandomSparkConfig: Map[String, String] = {
    if (config.hasPath(RQGConfig.SPARK_CONF_TEMPLATE)) {
      config.getConfig(RQGConfig.SPARK_CONF_TEMPLATE).entrySet().asScala.map { v =>
        v.getKey -> RandomUtils.nextChoice(config.getStringList(s"${RQGConfig.SPARK_CONF_TEMPLATE}.${v.getKey}").asScala.toArray)
      }.toMap
    } else {
      Map.empty
    }
  }

  def getReferenceSparkConfig: Map[String, String] = {
    getSparkConfig(RQGConfig.COMMON_SPARK_CONF) ++ getSparkConfig(RQGConfig.REFERENCE_SPARK_CONF)
  }

  def getTestSparkConfig: Map[String, String] = {
    getSparkConfig(RQGConfig.COMMON_SPARK_CONF) ++ getSparkConfig(RQGConfig.TEST_SPARK_CONF)
  }

  /**
   * Returns the list of whitelisted expressions. If no list is specified, returns `None`, which
   * represents all expressions being active.
   */
  def getWhitelistExpressions: Option[Seq[String]] = {
    if (config.hasPath(RQGConfig.EXPRESSIONS)) {
      Some(config.getStringList(RQGConfig.EXPRESSIONS).asScala)
    } else {
      None
    }
  }

  def withValue(path: String, value: ConfigValue) = {
    new RQGConfig(config.withValue(path, value))
  }
}

object RQGConfig {
  private val defaultDataTypeWeights =
    WeightEntry("Int", 10d) :: WeightEntry("TinyInt", 2d) :: WeightEntry("SmallInt", 2d) ::
      WeightEntry("BigInt", 2d) :: WeightEntry("Float", 2d) :: WeightEntry("Double", 5d) ::
      WeightEntry("Boolean", 1d) :: WeightEntry("Decimal", 10d) :: WeightEntry("String", 2d) ::
      WeightEntry("Date", 2d) :: WeightEntry("Timestamp", 2d) ::
      WeightEntry("Array", 2d) :: WeightEntry("Map", 2d) :: WeightEntry("Struct", 2d) :: Nil

  // Config keys
  val DATA_GENERATOR_PROFILE = "DATA_GENERATOR_PROFILE"
  val QUERY_PROFILE = "QUERY_PROFILE"
  val SPARK_CONF = "SPARK_CONF"
  val COMMON_SPARK_CONF = s"$SPARK_CONF.COMMON"
  val REFERENCE_SPARK_CONF = s"$SPARK_CONF.REFERENCE"
  val TEST_SPARK_CONF = s"$SPARK_CONF.TEST"
  // spark config key template with "knobs" as values
  // we use this to randomly sample spark conf
  val SPARK_CONF_TEMPLATE = "SPARK_CONF_TEMPLATE"

  /** ----------------- QUERY PROFILE ------------------- */

  // Category of query profile
  val BOUNDS = s"$QUERY_PROFILE.BOUNDS"
  val QUERY_WEIGHTS = s"$QUERY_PROFILE.WEIGHTS"
  val PROBABILITIES = s"$QUERY_PROFILE.PROBABILITIES"
  val EXPRESSIONS = s"$QUERY_PROFILE.EXPRESSIONS"

  // Bounds
  val MAX_NESTED_QUERY_COUNT = RQGConfigEntry(s"$BOUNDS.MAX_NESTED_QUERY_COUNT", (0, 2))
  val MAX_NESTED_COMPLEX_DATA_TYPE_COUNT = RQGConfigEntry(s"$BOUNDS.MAX_NESTED_COMPLEX_DATA_TYPE_COUNT", (0, 2))
  val MAX_COMPLEX_DATA_TYPE_LENGTH = RQGConfigEntry(s"$BOUNDS.MAX_COMPLEX_DATA_TYPE_LENGTH", (0, 5))
  val MAX_NESTED_EXPR_COUNT = RQGConfigEntry(s"$BOUNDS.MAX_NESTED_EXPR_COUNT", (0, 5))
  val SELECT_ITEM_COUNT = RQGConfigEntry(s"$BOUNDS.SELECT_ITEM_COUNT", (1, 5))
  val JOIN_COUNT = RQGConfigEntry(s"$BOUNDS.JOIN_COUNT", (0, 2))

  // Weights
  private val defaultJoinWeights =
    WeightEntry("CROSS", 0.01d) :: WeightEntry("FULL_OUTER", 0.04d) ::
      WeightEntry("INNER", 0.7d) :: WeightEntry("LEFT", 0.2d) :: WeightEntry("RIGHT", 0.05d) :: Nil
  val JOIN_TYPE = RQGConfigEntry(s"$QUERY_WEIGHTS.JOIN_TYPE", defaultJoinWeights)

  val QUERY_DATA_TYPE = RQGConfigEntry(s"$QUERY_WEIGHTS.DATA_TYPE", defaultDataTypeWeights)

  // Probabilities
  val WITH = RQGConfigEntry(s"$PROBABILITIES.WITH", 0.0d)
  val FROM = RQGConfigEntry(s"$PROBABILITIES.FROM", 1.0d)
  val WHERE = RQGConfigEntry(s"$PROBABILITIES.WHERE", 0.9d)
  val GROUP_BY = RQGConfigEntry(s"$PROBABILITIES.GROUP_BY", 0.1d)
  val HAVING = RQGConfigEntry(s"$PROBABILITIES.HAVING", 0.25d)
  val UNION = RQGConfigEntry(s"$PROBABILITIES.UNION", 0.1d)
  val ORDER_BY = RQGConfigEntry(s"$PROBABILITIES.ORDER_BY", 0.1d)

  val SELECT_DISTINCT = RQGConfigEntry(s"$PROBABILITIES.SELECT_DISTINCT", 0.5d)
  val NESTED_IN = RQGConfigEntry(s"$PROBABILITIES.NESTED_IN", 0.9d)
  val DISTINCT_IN_FUNCTION = RQGConfigEntry(s"$PROBABILITIES.DISTINCT_IN_FUNCTION", 0.5d)

  /** ----------------- DATA GENERATOR PROFILE ------------------- */

  // Category of data generator profile
  val DATA_GENERATOR_PROBABILITIES = s"$DATA_GENERATOR_PROFILE.PROBABILITIES"

  val DATA_GENERATOR_NULL = RQGConfigEntry(s"$DATA_GENERATOR_PROBABILITIES.NULL", 0.1d)

  def load(path: String = ""): RQGConfig = {
    if (path.isEmpty) {
      new RQGConfig(ConfigFactory.load("rqg-defaults.conf"))
    } else {
      new RQGConfig(ConfigFactory.load(path))
    }
  }
}

case class RQGConfigEntry(key: String, defaultValue: Any)
case class WeightEntry(key: String, value: Double)
