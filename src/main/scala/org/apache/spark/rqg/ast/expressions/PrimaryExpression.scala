package org.apache.spark.rqg.ast.expressions

import scala.collection.mutable
import org.apache.spark.rqg._
import org.apache.spark.rqg.ast.relations.{JoinCriteria, RelationPrimary}
import org.apache.spark.rqg.ast.{AggPreference, Column, ExpressionGenerator, Function, Functions, NestedQuery, QueryContext, TreeNode}
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
    val filteredChoices = if (querySession.needGenerateAggFunction) {
      choices.filter(_.canGenerateAggFunc)
    } else if (querySession.needGenerateColumnExpression) {
      choices.filter(_ == ColumnReference)
    } else if (querySession.allowedNestedSubQueryCount <= 0) {
      choices.filterNot(_ == SubQuery)
    } else if (querySession.needGeneratePrimitiveExpression) {
      choices.filter(_.canGeneratePrimitive)
    } else {
      choices
    }

    // Throw out expressions that can't produce the type we want.
    var finalChoices = filterChoicesForRequiredType[PrimaryExpression](
      querySession, requiredDataType, filteredChoices.toSeq)
        .filter {
          case colRef: ColumnReference =>
            // Column references are unique in that we need the type to match exactly.
            colRef.possibleDataTypes(querySession).exists(_.sameType(requiredDataType))
          case _ => true
        }
    // If there are options other than FunctionCall available for leaf expressions, don't generate
    // constants unnecessarily.
    if (finalChoices.exists(expr => expr == ColumnReference || expr == MakeStruct
        || expr == MakeArray)) {
      finalChoices = finalChoices.filterNot(_ == Constant)
    }

    val random = RandomUtils.nextChoice(finalChoices.toArray)
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

  def choices = Array(ColumnReference, Star, FunctionCall, Constant, MakeStruct, MakeArray)

  override def canGenerateNested: Boolean = choices.exists(_.canGenerateNested)

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

  private def toSqlString(element: Any, dataType: DataType[_]): String = dataType match {
    case StringType => s"'${element.toString}'"
    case DateType => s"to_date('${element.toString}')"
    case TimestampType => s"to_timestamp('${element.toString}')"
    case a: ArrayType =>
      val elements = element.asInstanceOf[Array[Any]].map(toSqlString(_, a.innerType))
      s"array(${elements.mkString(",")})"
    case m: MapType =>
      val map = element.asInstanceOf[mutable.Map[Any, Any]]
      val elements = map.map { case (key, value) =>
        val res = s"${toSqlString(key, m.keyType)},${toSqlString(value, m.valueType)}"
        res
      }
      s"map(${elements.mkString(",")})"
    case s: StructType =>
      val row = element.asInstanceOf[Row]
      // Use the `named_struct` expression, since we need the struct literal to have the correct
      // field names.
      val elements = row.toSeq.zip(s.fields).map { case (elem, field) =>
        s"'${field.name}', ${toSqlString(elem, field.dataType)}"
      }
      s"named_struct(${elements.mkString(", ")})"
    case _ =>
      // Add an explicit cast for constants to prevent constant-type-inference in the analyzer
      // from choosing something incorrect later.
      s"cast(${element.toString} as ${dataType.toSql})"
  }

  override def sql: String = toSqlString(value, requiredDataType)

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
    DataType.supportedDataTypes(querySession.rqgConfig)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}


/**
 * MakeStruct expression, which creates new structs from columns or literals.
 *
 * TODO(shoumik): We should generalize this to create any nested type.
 */
class MakeStruct(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {
  require(requiredDataType.isInstanceOf[StructType])


  val structType = requiredDataType.asInstanceOf[StructType]

  val fields: Seq[ValueExpression] = {
    queryContext.allowedNestedExpressionCount -= 1
    val result = structType.fields.zipWithIndex.map { case (field, i) =>
      ValueExpression(queryContext, Some(this), field.dataType,
        isLast = i == structType.fields.length)
    }
    queryContext.allowedNestedExpressionCount += 1
    result
  }

  override def sql: String = {
    val elements = fields.zip(structType.fields).map { case (expr, field) =>
      s"'${field.name}', ${expr.sql}"
    }
    s"named_struct(${elements.mkString(", ")})"
  }

  override def name: String = "make_struct"
  override def dataType: DataType[_] = requiredDataType
  override def isAgg: Boolean = fields.exists(_.isAgg)
  override def columns: Seq[ColumnReference] = fields.flatMap(_.columns)
  override def nonAggColumns: Seq[ColumnReference] = fields.flatMap(_.nonAggColumns)
}

/**
 * MakeStruct generator.
 */
object MakeStruct extends ExpressionGenerator[MakeStruct] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): MakeStruct = {
    new MakeStruct(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = false

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    // Struct, or no possible data types if struct is disabled.
    DataType.supportedDataTypes(querySession.rqgConfig).filter(_.isInstanceOf[StructType])
  }

  override def canGenerateRelational: Boolean = false
  override def canGenerateNested: Boolean = true
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
 * MakeArray expression, which creates new arrays from columns or literals.
 */
class MakeArray(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {
  require(requiredDataType.isInstanceOf[ArrayType])


  val arrayType = requiredDataType.asInstanceOf[ArrayType]


  val values: Seq[ValueExpression] = {
    queryContext.allowedNestedExpressionCount -= 1
    val numValues = RandomUtils.nextInt(5)
    val result = (0 to numValues).map(_ =>
      ValueExpression(queryContext, Some(this), arrayType.innerType))
    queryContext.allowedNestedExpressionCount += 1
    result
  }

  override def sql: String = {
    s"array(${values.map(_.sql).mkString(", ")})"
  }

  override def name: String = "make_array"
  override def dataType: DataType[_] = requiredDataType
  override def isAgg: Boolean = values.exists(_.isAgg)
  override def columns: Seq[ColumnReference] = values.flatMap(_.columns)
  override def nonAggColumns: Seq[ColumnReference] = values.flatMap(_.nonAggColumns)
}

/**
 * MakeArray generator.
 */
object MakeArray extends ExpressionGenerator[MakeArray] {
  override def apply(
      querySession: QueryContext,
      parent: Option[TreeNode],
      requiredDataType: DataType[_],
      isLast: Boolean): MakeArray = {
    new MakeArray(querySession, parent, requiredDataType)
  }

  override def canGeneratePrimitive: Boolean = true

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    // Array, or no possible data types if struct is disabled.
    DataType.supportedDataTypes(querySession.rqgConfig).filter(_.isInstanceOf[ArrayType])
  }

  override def canGenerateRelational: Boolean = false
  override def canGenerateNested: Boolean = true
  override def canGenerateAggFunc: Boolean = false
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
    val functions = (if (queryContext.aggPreference == AggPreference.FORBID) {
      supportedFunctions.filterNot(_.isAgg)
    } else if (queryContext.needGenerateAggFunction) {
      supportedFunctions.filter(_.isAgg)
    } else if (notValid(parent)) {
      supportedFunctions.filterNot(_.nondeterministic)
    } else {
      supportedFunctions
    }).toArray
    RandomUtils.nextChoice(
      functions.filter(f => f.returnType.isInstanceOf[GenericNamedType] ||
        f.returnType.sparkType.sameType(requiredDataType.sparkType)))
  }

  /**
   * Generate arguments for the selected expression, resolving any generics along the way.
   */
  protected def generateArguments: Seq[Expression] = {
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
  override def sql: String = {
    if (func.isAgg &&
        func.name != "first" &&
        arguments.size < 2 &&
        RandomUtils.nextBoolean(
          queryContext.rqgConfig.getProbability(RQGConfig.DISTINCT_IN_FUNCTION))) {
      s"${func.name}(distinct ${arguments.map(_.sql).mkString(", ")})"
    } else if (func.name == "case_when") {
      assert(arguments.size % 2 == 1 && arguments.size > 1)
      // Partition into when and then expressions. The last argument is the ELSE; the remaining
      // arguments alternate between the WHEN Expression and the THEN expression.
      val (whenExprs, thenExprs) = arguments.dropRight(1).zipWithIndex.partition(_._2 % 2 == 0)
      val cases = whenExprs.zip(thenExprs).map {
        case ((whenExpr, _), (thenExpr, _)) => s" WHEN ${whenExpr.sql} THEN ${thenExpr.sql}"
      }.mkString
      val elseCase = " ELSE " + arguments.last.sql
      "(CASE" + cases + elseCase + " END)"
    } else if (func.name == "is_null") {
      s"(${arguments(0).sql} IS NULL)"
    } else {
      s"${func.name}(${arguments.map(_.sql).mkString(", ")})"
    }
  }

  override def name: String = s"func_${dataType.fieldName}"

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
      isLast: Boolean): FunctionCall = {
    new FunctionCall(querySession, parent, requiredDataType, isLast)
  }

  override def possibleDataTypes(querySession: QueryContext): Array[DataType[_]] = {
    (if (querySession.aggPreference == AggPreference.FORBID) {
      querySession.allowedFunctions.filterNot(_.isAgg)
    } else {
      querySession.allowedFunctions
    }).map(_.returnType).distinct
  }.toArray

  override def canGeneratePrimitive: Boolean = false
  override def canGenerateRelational: Boolean = false
  override def canGenerateNested: Boolean = true
  override def canGenerateAggFunc: Boolean = true
}



/**
 * A reference to a column in an available table. This can either be a root column reference (e.g.,
 * `table_4.column_5` or a nested column reference from a STRUCT (e.g., `table_5.column_5.col1`).
 */
class ColumnReference(
    val queryContext: QueryContext,
    val parent: Option[TreeNode],
    requiredDataType: DataType[_]) extends PrimaryExpression {

  val column: Column = generateColumn

  private def generateRelation = {
    if (queryContext.needColumnFromJoiningRelation) {
      queryContext.joiningRelation.getOrElse {
        throw new IllegalArgumentException("No JoiningRelation exists to choose Column")
      }
    } else {
      RandomUtils.nextChoice(
        queryContext.availableRelations
          .filter(_.flattenedColumns.exists(c => requiredDataType.sameType(c.dataType))))
    }
  }

  private def generateColumn = {
    if (queryContext.availableColumns.isDefined) {
      val columns = queryContext.availableColumns.get.filter(c => requiredDataType.sameType(c.dataType))
      RandomUtils.nextChoice(columns)
    } else {
      val relation: RelationPrimary = generateRelation
      val columns = relation.flattenedColumns.filter(c => requiredDataType.sameType(c.dataType))
      RandomUtils.nextChoice(columns)
    }
  }

  override def sql: String = column.sql

  override def name: String = column.sql

  override def dataType: DataType[_] = column.dataType

  override def isAgg: Boolean = false

  override def columns: Seq[ColumnReference] = Seq(this)

  override def nonAggColumns: Seq[ColumnReference] =
    columns.filterNot(c => c.dataType.isInstanceOf[MapType])
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
    DataType.supportedDataTypes(querySession.rqgConfig)
  }

  override def canGenerateRelational: Boolean = false

  override def canGenerateNested: Boolean = false

  override def canGenerateAggFunc: Boolean = false
}
