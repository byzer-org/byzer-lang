package tech.mlsql

/**
  * 2019-04-18 WilliamZhu(allwefantasy@gmail.com)
  */
object Stage extends Enumeration {
  type stage = Value
  val include = Value("include")
  val preProcess = Value("preProcess")
  val auth = Value("auth")
  val physical = Value("physical")
  val grammar = Value("grammar")
}
