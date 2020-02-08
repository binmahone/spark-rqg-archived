package com.baidu.spark.rqg

package object ast {
  case class Operator(name: String, op: String)

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
}
