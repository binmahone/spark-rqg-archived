package com.baidu.spark.rqg

object RQGUtils {

  def groupTableExprByColType(tableExprs: Array[TableExpr]): Map[DataType, Array[TableExpr]] = {
    tableExprs
      .flatMap(x => x.cols.map(y => (x, y.dataType)))
      .groupBy(_._2)
      .mapValues(x => x.map(_._1))
  }

  def groupValExprByColType(valExprs: Array[ValExpr]): Map[DataType, Array[ValExpr]] = {
    valExprs
      .map(x => (x, x.dataType))
      .groupBy(_._2)
      .mapValues(x => x.map(_._1))
  }
}
