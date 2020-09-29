package tech.mlsql.lang.cmd.compile.internal.gc

sealed abstract class Token
(
  val _value: String
)

case object BRACE_LEFT extends Token("{")

case object BRACE_RIGHT extends Token("}")

case object SQUARE_LEFT extends Token("[")

case object SQUARE_RIGHT extends Token("]")

case object PARENTHEDIS_LEFT extends Token("(")

case object PARENTHEDIS_RIGHT extends Token(")")

case class VARIABLE(id: String) extends Token("")

case class CONST_NUMBER(value: Double) extends Token("")

case class CONST_STRING(value: String) extends Token("")

case class IDENT(value: String) extends Token("")

case object OP_AND extends Token("and")

case object OP_OR extends Token("or")


class Lexer(data: Array[Char], n: Int, origin: String) {
  var p = 0
  var line = 1
  var charPositionInLine = 0
  var markDepth = 0

  def reset() = {
    p = 0
    line = 1
    charPositionInLine = 0
    markDepth = 0
  }

  def parse(): Unit = {
    for (p <- 0 until n) {
        data(p) match {
          case '(' =>
        }
    }
  }
}

object Lexer {
  def parse(source: String): List[Token] = {
    val data = source.toCharArray
    val n = source.length
    val lexer = new Lexer(data, n, source)
    lexer.parse()

  }
}
