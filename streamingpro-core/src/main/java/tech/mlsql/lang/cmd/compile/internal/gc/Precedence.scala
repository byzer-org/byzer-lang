package tech.mlsql.lang.cmd.compile.internal.gc

/**
 * 3/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object Precedence {
  val MESSAGE     = 10
  val UNARY       =  9  // not -
  val PRODUCT     =  8  // * / %
  val TERM        =  7  // + -
  val COMPARISON  =  5  // < > <= >=
  val EQUALITY    =  4  // == !=
  val RECORD      =  3  // ,
  val ASSIGNMENT  =  2
  val LOGICAL     =  1  // and or
}
