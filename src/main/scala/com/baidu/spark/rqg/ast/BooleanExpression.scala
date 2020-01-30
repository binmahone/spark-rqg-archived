package com.baidu.spark.rqg.ast

import com.baidu.spark.rqg.DataType

class BooleanExpression(querySession: QuerySession, parent: Option[TreeNode] = None) extends TreeNode(querySession, parent) {
  // Step 1: random pick a common data type
  // Think: what if there is no common data type?
  private val dataType: DataType[_] = {
    val leftDataTypes = querySession.primaryRelations.flatMap(relation =>
      querySession.tables.find(_.name == relation.tableIdentifier)
    ).flatMap(_.columns).map(_.dataType)

    val rightDataTypes = querySession.joiningRelations.flatMap(relation =>
      querySession.tables.find(_.name == relation.tableIdentifier)
    ).flatMap(_.columns).map(_.dataType)

    val dataTypes = leftDataTypes.intersect(rightDataTypes).distinct
    assert(dataTypes.nonEmpty, "left and right relations has no common data type to join")
    dataTypes(random.nextInt(dataTypes.length))
  }

  // Step 2: random pick a table has one column with target data type
  private val leftRelations = querySession.primaryRelations.filter { relation =>
    querySession.tables
      .find(_.name == relation.tableIdentifier)
      .get.columns
      .exists(_.dataType == dataType)
  }
  private val leftRelation = leftRelations(random.nextInt(leftRelations.length))

  private val rightRelations = querySession.joiningRelations.filter { relation =>
    querySession.tables
      .find(_.name == relation.tableIdentifier)
      .get.columns
      .exists(_.dataType == dataType)
  }
  private val rightRelation = rightRelations(random.nextInt(rightRelations.length))

  // Step 3: random pick a column with target data type
  private val leftColumns = querySession.tables.find(_.name == leftRelation.tableIdentifier)
    .get.columns.toArray
  val leftColumn = leftColumns(random.nextInt(leftColumns.length))

  private val rightColumns = querySession.tables.find(_.name == rightRelation.tableIdentifier)
    .get.columns.toArray
  val rightColumn = rightColumns(random.nextInt(rightColumns.length))

  // Step 4: set column identifier
  val left = s"${leftRelation.aliasIdentifier.getOrElse(leftRelation.tableIdentifier)}.${leftColumn.name}"
  val right = s"${rightRelation.aliasIdentifier.getOrElse(rightRelation.tableIdentifier)}.${rightColumn.name}"

  override def toSql: String = s"$left == $right"
}

