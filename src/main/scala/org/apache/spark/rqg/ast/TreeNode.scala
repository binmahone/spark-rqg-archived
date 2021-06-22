package org.apache.spark.rqg.ast

import org.apache.spark.rqg._
import org.apache.spark.rqg.ast.relations.RelationPrimary

/**
 * A TreeNode represents a part of a Query.
 * A set of TreeNode will form an AST and represent a complete Query. Here is an example:
 *
 *                          -------- Query --------------
 *                         /                             \
 *               SelectClause                           FromClause
 *            /           \                           /           \
 * SetQuantifier  NamedExpression                  TablePrimary   JoinRelation
 *     |          /        \ <-> create as a child
 * "DISTINCT"  Alias    BooleanExpression (trait)
 *             /            \ <-> random choose a sub-class
 *        "expr_1"       Predicated (class extends BooleanExpression)
 *                           \ <-> create as a child
 *                        ValueExpression (trait)
 *                            \ <-> random choose a sub-class
 *                         PrimaryExpression (trait extends ValueExpression)
 *                             \ <-> random choose a sub-class
 *                          Column (class extends PrimaryExpression)
 *
 * This example contains 2 kinds of node creation:
 * 1. parent node create a child node, e.g. NamedExpression create a BooleanExpression
 * 2. abstract node create a sub-class node, e.g. PrimaryExpression create a Column
 *
 * TODO: add a check to make sure the AST is consistent to target spark version. (maybe comparing
 * the commit id of sqlbase.g4)
 */
trait TreeNode {
  def parent: Option[TreeNode]
  def queryContext: QueryContext
  def sql: String
}

/**
 * QueryContext contains all the states and requirements during a Query generating, such as:
 * 1. available tables a FromClause can choose from
 * 2. aliasId to generate unique alias
 * 3. allowed data types when generating an expression
 * 4. required nested expression count
 */
case class QueryContext(
    var rqgConfig: RQGConfig = RQGConfig.load(),
    var availableTables: Array[Table] = Array.empty,
    var availableRelations: Array[RelationPrimary] = Array.empty,
    var joiningRelation: Option[RelationPrimary] = None,
    var allowedNestedExpressionCount: Int = 5,
    var allowedNestedSubQueryCount: Int = 2,
    var requiredRelationalExpressionCount: Int = 0,
    var requiredColumnCount: Int = 0,
    var needColumnFromJoiningRelation: Boolean = false,
    var aggPreference: Int = AggPreference.FORBID,
    var nextAliasId: Int = 0) {

  lazy val allowedFunctions: Seq[Function] = {
    rqgConfig.getWhitelistExpressions match {
      case Some(whitelist) =>
        // Only allow functions that are in the whitelist.
        val normalizedWhitelist = whitelist.map(_.toLowerCase().trim)
        Functions.getFunctions.filter(
          func => normalizedWhitelist.contains(func.name.toLowerCase().trim))
      case None => Functions.getFunctions
    }
  }

  def nextAlias(prefix: String): String = {
    val id = nextAliasId
    nextAliasId += 1
    s"${prefix}_alias_$id"
  }

  def needGenerateRelationalExpression: Boolean = {
    requiredRelationalExpressionCount > 0 &&
      requiredRelationalExpressionCount == allowedNestedExpressionCount
  }

  def needGeneratePrimitiveExpression: Boolean = {
    allowedNestedExpressionCount <= 0
  }

  def needGenerateColumnExpression: Boolean = {
    needGeneratePrimitiveExpression && requiredColumnCount > 0
  }

  def needGenerateAggFunction: Boolean = {
    aggPreference == AggPreference.PREFER && allowedNestedExpressionCount == 1
  }

  def dataTypesInAvailableRelations: Array[DataType[_]] = {
    val supportedDataTypes = DataType.supportedDataTypes(rqgConfig)
    val typesInRelations = availableRelations.flatMap(_.flattenedColumns).map(_.dataType)
    // This is all non-parameterized types.
    val allowedNoParameters = supportedDataTypes.filterNot(_.hasParameters).intersect(typesInRelations)
    // All available types that have parameters/nested types.
    val allowedWithParameters = supportedDataTypes.filter(_.hasParameters).flatMap {
      case _: MapType => typesInRelations.filter(_.isInstanceOf[MapType])
      case _: StructType => typesInRelations.filter(_.isInstanceOf[StructType])
      case _: ArrayType => typesInRelations.filter(_.isInstanceOf[ArrayType])
      case _: DecimalType => typesInRelations.filter(_.isInstanceOf[DecimalType])
      case _ => Seq()
    }
    (allowedNoParameters ++ allowedWithParameters).distinct
  }

  /**
   * Given the current join relation and available relations, returns a list of types that we
   * can produce a join condition on. We never produce joins over boolean columns, because they can
   * quickly lead to blow-up/cross join conditions.
   */
  def commonDataTypesForJoin: Array[DataType[_]] = {
    // Find data type that is in the joining relation and in the available relations.
    val joiningRelationTypes = joiningRelation.map(_.flattenedColumns.map(_.dataType))
    // Find which data types in the joining relation are also in other available relations.
    joiningRelationTypes.map(_.intersect(dataTypesInAvailableRelations))
      .getOrElse(dataTypesInAvailableRelations)
      .filter(_.isJoinable)
      .distinct
  }
}

/**
 * Represents a table from db
 */
case class Table(name: String, columns: Array[Column]) {
  def sameTable(other: Table): Boolean = {
    name == other.name && columns.sameElements(other.columns)
  }

  override def toString: String = {
    s"$name - ${columns.mkString(", ")}"
  }

  def schemaString: String = {
    columns.map(column => s"${column.name} ${column.dataType.toSql}").mkString(", ")
  }
}

/**
 * Represents a column from db
 */
case class Column(tableName: String, name: String, dataType: DataType[_]) {
  def sql: String = s"$tableName.$name"
}

object AggPreference {
  val PREFER = 0
  val ALLOW = 1
  val FORBID = 2
}
