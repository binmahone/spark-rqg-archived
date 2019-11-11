package com.baidu.spark.rqg

class QueryGenerator(queryProfile: QueryProfile) {

  def createQuery(
      tableExprs: Array[TableExpr],
      requiredTableExprColType: Option[DataType] = None,
      tableAliasPrefix: String = "t"): Query = {

    val fromClause = createFromClause(tableExprs, tableAliasPrefix, requiredTableExprColType)
    val selectClause = createSelectClause(tableExprs)
    new Query(
      selectClause = selectClause,
      fromClause = fromClause)
  }

  private def createFromClause(
      tableExprs: Array[TableExpr],
      tableAliasPrefix: String,
      requiredTableExprColType: Option[DataType]): FromClause = {

    // 1. choose table count from profile (TODO)
    // 2. loop for table count
    //   2.1. first loop, choose table expr and create from clause
    //      2.1.1. get candidate table exprs based on required type
    //      2.1.2. get candidate by joinalbe type if need (TODO)
    //      2.1.3. create table expr based on table expr list
    //   2.2. rest loops, create join clause (TODO)
    // 3. remove alias if only one table expr created (TODO)

    // val tableCount = queryProfile.getTableCount
    val tableCount = 1

    val candidateTableExprs = if (requiredTableExprColType.isDefined) {
      tableExprs.filter(tableExpr => tableExpr.cols.map(_.dataType).contains(requiredTableExprColType.get))
    } else {
      tableExprs
    }
    val tableExpr = createTableExpr(candidateTableExprs)
    tableExpr.alias = Some(tableAliasPrefix + "1")

    (1 until tableCount).foreach { idx =>
      // create join clause
    }

    FromClause(tableExpr)
  }

  private def createTableExpr(
      tableExprs: Array[TableExpr], requiredType: Option[DataType] = None): TableExpr = {

    // 1. create inline view if allow more nested queries and support use inline view (TODO)
    // 2. get candidate table exprs based on required type (TODO is_subclass)
    // 3. choose one table expr using profile (TODO)
    val candidateTableExprs = if (requiredType.isDefined) {
      tableExprs.filter(tableExpr => tableExpr.cols.map(_.dataType).contains(requiredType.get))
    } else {
      tableExprs
    }
    candidateTableExprs.head
  }

  private def createSelectClause(
      tableExprs: Array[TableExpr],
      selectItemDataTypes: Array[DataType] = Array.empty,
      requiredSelectItemType: Option[DataType] = None,
      useAggSubquery: Boolean = false): SelectClause = {

    // 1. check selectItemDataTypes and requiredSelectItemType
    // 2. Generate column types for the select clause if selectItemDataTypes not specified.
    //   2.1. length of selectItemDataTypes means length of columns
    // 3. randomly assign AGG to some columns
    // 4. randomly assign BASIC and ANALYTIC to some columns
    // 5. transform BASIC, AGG, ANALYTIC to its SelectItem
    //   5.1. first pass, transform BASIC to basic SelectItem, add data type to other categories
    //   5.2. add generated basic SelectItem.valExpr to selectItemExprList
    //   5.3. second pass, trasnform AGG to agg SelectItem based on selectItemExprList
    //   5.4. add generated agg SelectItem.valExpr to selectItemExprList
    //   5.5. third pass, transform ANALYTIC to analytic SelectItem based on selectItemExprList
    // 6. add alias for simple column reference if there is conflict, add alias for all others
    val finalSelectItemDataTypes = Array(IntType)
    val columnCategories = Array("BASIC", "AGG", "ANALYTIC")

    val aggPresent = columnCategories.contains("AGG")
    val analyticCount = columnCategories.count(_ == "ANALYTIC")

    val selectItems = finalSelectItemDataTypes.map { x =>
      createBasicSelectItem(tableExprs, x)
    }
    SelectClause(selectItems)
  }

  private def createBasicSelectItem(tableExprs: Array[TableExpr], itemType: DataType): SelectItem = {
    // 1. choose nested expr count from profile (TODO)
    // 2. create basic SelectItem
    //   2.1. create SelectItem with func if count > 0 (TODO)
    //   2.2. choose a column with specific itemType (TODO)
    //   2.3. chosse a constant with specific itemType if no column (TODO)
    val maxChildren = 0
    val valExpr = if (maxChildren > 0) {
      tableExprs.head.cols.head
    } else {
      val columns = tableExprs.flatMap(_.cols).filter(_.dataType == itemType)
      if (columns.nonEmpty) {
        columns.head
      } else {
        columns.head
      }
    }
    SelectItem(valExpr)
  }
}
