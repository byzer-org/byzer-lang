package tech.mlsql.lang.cmd.compile.internal.gc

object Types extends Enumeration {
  type DataType = Value
  val Int = Value("Int")
  val Float = Value("Float")
  val Char = Value("Char")
  val String = Value("String")
  val Boolean = Value("boolean")
  val Any = Value("any")
}
