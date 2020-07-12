package org.apache.spark.sql.types

import scala.util.Random

object SparkSQLPrivateHelper {
  def getHelp(collectionType: Any, dataTypes: Seq[DataType], random: Random): DataType = {
    val instance = collectionType.asInstanceOf[TypeCollection]
    val accepts = dataTypes.filter(dt => instance.acceptsType(dt))
    accepts(random.nextInt(accepts.size))
  }
}
