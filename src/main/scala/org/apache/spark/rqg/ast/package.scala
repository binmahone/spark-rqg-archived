package org.apache.spark.rqg

package object ast {
  case class Operator(name: String, op: String)

  case class Function(
    name: String,
    returnType: DataType[_],
    inputTypes: Seq[DataType[_]],
    isAgg: Boolean,
    nondeterministic: Boolean) {
    override def toString = s"${name}(${inputTypes.mkString(",")}) -> ${returnType}"
  }

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

  object joins {
    val INNER = JoinType("INNER", "INNER")
    val CROSS = JoinType("CROSS", "CROSS")
    val LEFT = JoinType("LEFT OUTER", "LEFT")
    val RIGHT = JoinType("RIGHT OUTER", "RIGHT")
    val FULL = JoinType("FULL OUTER", "FULL_OUTER")
  }
}
