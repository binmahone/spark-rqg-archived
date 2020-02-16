package com.baidu.spark.rqg

package object ast {
  case class Operator(name: String, op: String)

  case class Function(name: String, signatures: Seq[Signature], isAgg: Boolean)

  case class Signature(returnType: DataType[_], inputTypes: Seq[DataType[_]])

  case class JoinType(name: String, weightName: String) extends WeightedChoice

  object operators {
    val AND = Operator("and", "AND")
    val OR = Operator("or", "OR")
    val MINUS = Operator("minus", "-")
    val PLUS = Operator("plus", "+")
    val CONCAT = Operator("concat", "||")
    val TILDE = Operator("tilde", "~")
    val DIVIDE = Operator("divide", "/")
    val MULTIPLY = Operator("multiply", "*")
    val MOD = Operator("mod", "%")
    val EQ = Operator("eq", "==")
    val NEQ = Operator("neq", "<>")
    val NEQJ = Operator("neqj", "!=")
    val LT = Operator("lt", "<")
    val LTE = Operator("lte", "<=")
    val GT = Operator("gt", ">")
    val GTE = Operator("gte", ">=")
    val NSEQ = Operator("nseq", "<=>")
  }

  object functions {
    val COUNT = Function("count", Seq(Signature(BigIntType, Seq(IntType))), isAgg = true)
    val SUM = Function("sum",
      Seq(
        Signature(BigIntType, Seq(IntType)),
        Signature(DoubleType, Seq(FloatType)),
        Signature(DoubleType, Seq(DoubleType))), isAgg = true)
    val FIRST = Function("first",
      Seq(
        Signature(BooleanType, Seq(BooleanType)),
        Signature(IntType, Seq(IntType)),
        Signature(TinyIntType, Seq(TinyIntType)),
        Signature(SmallIntType, Seq(SmallIntType)),
        Signature(FloatType, Seq(FloatType)),
        Signature(DecimalType(), Seq(DecimalType())),
        Signature(StringType, Seq(StringType))), isAgg = true)
    val ABS = Function("abs", Seq(Signature(IntType, Seq(IntType))), isAgg = false)
  }

  object joins {
    val INNER = JoinType("INNER", "INNER")
    val CROSS = JoinType("CROSS", "CROSS")
    val LEFT = JoinType("LEFT OUTER", "LEFT")
    val RIGHT = JoinType("RIGHT OUTER", "RIGHT")
    val FULL = JoinType("FULL OUTER", "FULL_OUTER")
  }
}
