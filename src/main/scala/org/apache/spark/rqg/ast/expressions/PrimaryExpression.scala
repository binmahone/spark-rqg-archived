package org.apache.spark.rqg.ast.expressions

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rqg._
import org.apache.spark.rqg.ast.relations.{JoinCriteria, JoinRelation}
import org.apache.spark.rqg.ast.{AggPreference, ExpressionGenerator, Function, Functions, NestedQuery, QueryContext, TreeNode}
import org.apache.spark.sql.Row

/**
 * primaryExpression
 *      : name=(CURRENT_DATE | CURRENT_TIMESTAMP)                                                  #currentDatetime
 *      | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
 *      | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
 *      | CAST '(' expression AS dataType ')'                                                      #cast
 *      | STRUCT '(' (argument+=namedExpression (',' argument+=namedExpression)*)? ')'             #struct
 *      | (FIRST | FIRST_VALUE) '(' expression ((IGNORE | RESPECT) NULLS)? ')'                     #first
 *      | (LAST | LAST_VALUE) '(' expression ((IGNORE | RESPECT) NULLS)? ')'                       #last
 *      | POSITION '(' substr=valueExpression IN str=valueExpression ')'                           #position
 *      | constant                                                                                 #constantDefault
 *      | ASTERISK                                                                                 #star
 *      | qualifiedName '.' ASTERISK                                                               #star
 *      | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
 *      | '(' query ')'                                                                            #subqueryExpression
 *      | qualifiedName '(' (setQuantifier? argument+=expression (',' argument+=expression)*)? ')'
 *         (OVER windowSpec)?                                                                      #functionCall
 *      | IDENTIFIER '->' expression                                                               #lambda
 *      | '(' IDENTIFIER (',' IDENTIFIER)+ ')' '->' expression                                     #lambda
 *      | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
 *      | identifier                                                                               #columnReference
 *      | base=primaryExpression '.' fieldName=identifier                                          #dereference
 *      | '(' expression ')'                                                                       #parenthesizedExpression
 *      | EXTRACT '(' field=identifier FROM source=valueExpression ')'                             #extract
 *      | (SUBSTR | SUBSTRING) '(' str=valueExpression (FROM | ',') pos=valueExpression
 *        ((FOR | ',') len=valueExpression)? ')'                                                   #substring
 *      | TRIM '(' trimOption=(BOTH | LEADING | TRAILING)? (trimStr=valueExpression)?
 *         FROM srcStr=valueExpression ')'                                                         #trim
 *      | OVERLAY '(' input=valueExpression PLACING replace=valueExpression
 *        FROM position=valueExpression (FOR length=valueExpression)? ')'                          #overlay
 *
 * This is the class that generate concrete Expressions
 * For now, we only support column, constant, start, functionCall
 */
trait PrimaryExpression extends ValueExpression

/**
 * It random generate one class extends PrimaryExpression
 */
object PrimaryExpression extends ExpressionGenerator[PrimaryExpression] {
  def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean = false): PrimaryExpression = {

    val parentIsPredicate = checkIfParentIsPredicate(parent)

    val filteredChoices = (if (querySession.needGenerateAggFunction) {
      choices.filter(_.canGenerateAggFunc)
    } else if (querySession.needGenerateColumnExpression) {
      choices.filter(_ == ColumnReference)
    } else if (querySession.allowedNestedSubQueryCount <= 0 || parentIsPredicate) {
      var x = choices
      if (parentIsPredicate) {
        x = x.filterNot(_ == ColumnReference)
      }
      if (querySession.allowedNestedSubQueryCount <= 0) {
        x = x.filterNot(_ == SubQuery)
      }
      x
    } else if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }).filter(_.possibleDataTypes(querySession).exists(requiredDataType.acceptsType))

    val random = RandomUtils.nextChoice(filteredChoices)
    random.apply(querySession, parent, requiredDataType, isLast)
  }

  def checkIfParentIsPredicate(parent: Option[TreeNode]): Boolean = {
    if (parent.isEmpty) {
      return false
    }
    if (parent.get.isInstanceOf[Predicate]) {
      return true
    }
    checkIfParentIsPredicate(parent.get.parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def canGenerateRelational: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    choices.flatMap(_.possibleDataTypes(querySession)).distinct
  }

  def choices = Array(Constant, ColumnReference, Star, FunctionCall, SubQuery)

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = true
}

/**
 * Constant
 */
class Constant(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  val value: Any = generateValue

  private def generateValue = {
    RandomUtils.nextConstant(requiredDataType)
  }

  def getString (element: Any): String = {
    if (element.isInstanceOf[Array[Any]]) {
      val arr = element.asInstanceOf[Array[Any]]
      var res = ArrayBuffer[Any]()
      for (elem <- arr) {
        res += getString(elem)
      }
      s"array(${res.mkString(",")})"
    } else if (element.isInstanceOf[mutable.Map[Any, Any]]) {
      val map = element.asInstanceOf[mutable.Map[Any, Any]]
      val res = new StringBuilder()
      res.append("map(")
      for ((k, v) <- map) {
        res.append(getString(k))
        res.append(",")
        res.append(getString(v))
        res.append(",")
      }
      res.deleteCharAt(res.length - 1)
      res.append(")")
      res.toString()
    } else if (element.isInstanceOf[Row]) {
      val instance = element.asInstanceOf[Row]
      val length = instance.length - 1
      var res = ArrayBuffer[String]()
      for (i <- 0 to length) {
        res += getString(instance.get(i))
      }
      "struct(" + res.mkString(",") + ")"
    } else {
      element match {
        case s: String => s"'${s.toString}'"
        case _ => element.toString
      }
    }
  }

  override def sql: String = {
    requiredDataType match {
      case StringType => s"'${value.toString}'"
      case DateType => s"to_date('${value.toString}')"
      case TimestampType => s"to_timestamp('${value.toString}')"
      case a: ArrayType =>
        getString(value)
      case m: MapType =>
        getString(value)
      case s: StructType =>
        getString(value)
      case _ =>
        value.toString
    }
  }

  override def name: String = "constant"

  override def dataType: DataType[_] = requiredDataType

  override def isAgg: Boolean = {
    requiredDataType.isInstanceOf[MapType] || requiredDataType.isInstanceOf[StructType]
  }

  override def columns: Seq[ColumnReference] = Seq.empty

  override def nonAggColumns: Seq[ColumnReference] = Seq.empty
}

/**
 * Constant generator
 */
object Constant extends ExpressionGenerator[Constant] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Constant = {
    new Constant(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    querySession.allowedDataTypes
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

class Star(
    val queryContext: QueryContext,
    val parent: Option[TreeNode]) extends PrimaryExpression {
  override def sql: String = "*"

  override def name: String = "star"

  // TODO: what should we return?
  override def dataType: DataType[_] = ???

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = ???

  override def nonAggColumns: Seq[ColumnReference] = ???
}

/**
 * Star generator. This class is special since it has more than one data type
 */
object Star extends ExpressionGenerator[Star] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): Star = {
    new Star(querySession, parent)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    Array.empty
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

/**
 * grammar: qualifiedName '(' (setQuantifier? argument+=expression (',' argument+=expression)*)? ')'
 * All spark function call generated from here.
 */
class FunctionCall(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_],
    isLast: Boolean) extends PrimaryExpression {


  val func: Function = Functions.resolveGenerics(
    generateFunction, DataType.supportedDataTypes(queryContext.rqgConfig), requiredDataType)
  val arguments: Seq[Expression] = generateArguments

  /**
   * Check all parent to see if they don't satisfies these rules nondeterministic expressions are
   * only allowed in Project, Filter, Aggregate or Window This is the spark rule that only allows
   * nondeterministic function in certain clause only.
   *
   * @param parent ast parent
   * @return whether or not the function is not valid
   */
  private def notValid(parent: Option[TreeNode]): Boolean = {
    if (parent.isEmpty) return false
    parent.get match {
      case _: JoinCriteria => true
      case f: FunctionCall =>
        if (f.func.isAgg) true else notValid(parent.get.parent)
      case _ => notValid(parent.get.parent)
    }
  }

  /**
   * Generate the function to use.
   */
  protected def generateFunction: Function = {
    val supportedFunctions = queryContext.allowedFunctions
    val functions = (if (queryContext.aggPreference == AggPreference.FORBID) supportedFunctions.filterNot(_.isAgg) else if (queryContext.needGenerateAggFunction) supportedFunctions.filter(_.isAgg) else if (notValid(parent)) supportedFunctions.filterNot(_.nondeterministic) else supportedFunctions).toArray
    RandomUtils.nextChoice(
      functions.filter(f => f.returnType.isInstanceOf[GenericNamedType] ||
          f.returnType.sparkType.sameType(requiredDataType.sparkType)))
  }

  /**
   * Generate arguments for the selected expression, resolving any generics along the way.
   */
  private def generateArguments = {
    // Generics should be resolved by this point.
    require(!func.returnType.isInstanceOf[GenericNamedType])
    require(!func.inputTypes.exists(_.isInstanceOf[GenericNamedType]))
    queryContext.allowedNestedExpressionCount -= 1
    val previousAggPreference = queryContext.aggPreference
    if (func.isAgg) queryContext.aggPreference = AggPreference.FORBID
    val length = func.inputTypes.length
    val arguments = func.inputTypes.zipWithIndex.map { case (dt, idx) =>
      ValueExpression(queryContext, Some(this), dt, isLast = idx == (length - 1))
    }
    // Restore original preferences.
    queryContext.aggPreference = previousAggPreference
    queryContext.allowedNestedExpressionCount += 1
    arguments
  }

  /**
   * Converts the function call to SQL. Some expressions need special handling here
   * (e.g., CASE WHEN).
   *
   * TODO(shoumik): This should probably be refactored at some point...
   */
  override def sql: String = if (func.isAgg &&
      func.name != "first" &&
      arguments.size < 2 &&
      RandomUtils.nextBoolean(
        queryContext.rqgConfig.getProbability(RQGConfig.DISTINCT_IN_FUNCTION))) s"${func.name}(distinct ${arguments.map(_.sql).mkString(", ")})" else if (func.name == "case_when") {
    assert(arguments.size % 2 == 1 && arguments.size > 1)
    // Partition into when and then expressions. The last argument is the ELSE; the remaining
    // arguments alternate between the WHEN Expression and the THEN expression.
    val (whenExprs, thenExprs) = arguments.dropRight(1).zipWithIndex.partition(_._2 % 2 == 0)
    val cases = whenExprs.zip(thenExprs).map {
      case ((whenExpr, _), (thenExpr, _)) => s" WHEN ${whenExpr.sql} THEN ${thenExpr.sql}"
    }.mkString
    val elseCase = " ELSE " + arguments.last.sql
    "(CASE" + cases + elseCase + " END)"
  } else if (func.name == "is_null") s"(${arguments(0).sql} IS NULL)" else s"${func.name}(${arguments.map(_.sql).mkString(", ")})"

  override def name = s"func_${dataType.fieldName}"

  override def dataType: DataType[_] = func.returnType

  override def isAgg: Boolean = func.isAgg || arguments.exists(_.isAgg)

  override def columns: Seq[ColumnReference] = arguments.flatMap(_.columns)

  override def nonAggColumns: Seq[ColumnReference] = columns.filterNot(_.isAgg)
}

/**
 * FunctionCall generator
 */
object FunctionCall extends ExpressionGenerator[FunctionCall] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean) = new FunctionCall(querySession, parent, requiredDataType, isLast)

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    (if (querySession.aggPreference == AggPreference.FORBID) querySession.allowedFunctions.filterNot(_.isAgg) else querySession.allowedFunctions).map(_.returnType).distinct
  }.toArray

  override def canGeneratePrimitive = false

  override def canGenerateRelational = false

  override def canGenerateNested = true

  override def canGenerateAggFunc = true
}



/**
 * Random pick a column from available relations
 */
class ColumnReference(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  private val relation = generateRelation
  private val column = generateColumn

  private def generateRelation = {
    if (queryContext.needColumnFromJoiningRelation) {
      queryContext.joiningRelation.getOrElse {
        throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
      }
    } else {
      RandomUtils.nextChoice(
        queryContext.availableRelations
          .filter(_.columns.exists(c => requiredDataType.acceptsType(c.dataType))))
    }
  }

  private def generateColumn = {
    val columns = relation.columns.filter(c => requiredDataType.acceptsType(c.dataType))
    RandomUtils.nextChoice(columns)
  }

  override def sql: String = s"${relation.name}.${column.name}"

  override def name: String = s"${relation.name}_${column.name}"

  override def dataType: DataType[_] = column.dataType

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = Seq(this)

  override def nonAggColumns: Seq[ColumnReference] = columns.filterNot(c => c.dataType.isInstanceOf[MapType])
}

/**
 * ColumnReference generator
 */
object ColumnReference extends ExpressionGenerator[ColumnReference] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): ColumnReference = {
    new ColumnReference(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    querySession.dataTypesInAvailableRelations
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}

class SubQuery(
   val queryContext: QueryContext,
   val parent: Option[TreeNode],
   requiredDataType: DataType[_]) extends PrimaryExpression {

  private val subQuery = generateSubQuery

  private def generateSubQuery: NestedQuery = {
    NestedQuery(
      QueryContext(availableTables = queryContext.availableTables,
        rqgConfig = queryContext.rqgConfig,
        allowedNestedSubQueryCount = queryContext.allowedNestedSubQueryCount,
        nextAliasId = queryContext.nextAliasId + 1),
      Some(this),
      Some(requiredDataType))
  }

  override def sql: String = s"(${subQuery.sql})"

  override def name: String = "subQuery_primary"

  override def dataType: DataType[_] = requiredDataType

  // I think the value here does not really matters. For ex: if another expression
  // aggregate the subquery like MAX(SELECT ....) then that agg function call will return True
  // upward so the value here does not really matter.
  override def isAgg: Boolean = true

  override def columns: Seq[ColumnReference] = Seq.empty

  override def nonAggColumns: Seq[ColumnReference] = Seq.empty
}

/**
 * SubQuery generator
 */
object SubQuery extends ExpressionGenerator[SubQuery] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): SubQuery = {
    new SubQuery(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    DataType.supportedDataTypes
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}
