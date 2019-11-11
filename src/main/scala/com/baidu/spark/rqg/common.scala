package com.baidu.spark.rqg


trait ValExpr {
  def dataType: DataType
}

trait TableExpr {
  def identifier: String
  def cols: Array[Column]
  def alias: Option[String]
  def alias_=(newAlias: Option[String]): Unit
}

case class Table(
    name: String,
    columns: Array[Column],
    var alias: Option[String] = None)
  extends TableExpr {

  override def identifier: String = alias.getOrElse(name)

  override def cols: Array[Column] = columns
}

case class Column(name: String, dataType: DataType) extends ValExpr

case class Literal(value: Any, dataType: DataType) extends ValExpr
