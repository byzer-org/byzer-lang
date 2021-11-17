package tech.mlsql.lang.cmd.compile.internal.gc

import scala.collection.mutable.ArrayBuffer

case class StringInterpolationToken(name: String, chars: Array[Char])

class TextTemplate(template: Map[String, Any], str: String) {
  var srcPos = -1
  val srcEnd = str.length
  val srcChars = str.toCharArray

  var line = 1
  var column = 0


  def parse = {
    val buffer = new ArrayBuffer[StringInterpolationToken]()
    var wow = scan
    while (!wow.isEmpty) {
      buffer += wow.get
      wow = scan
    }
    buffer.toList
  }

  private def error(msg: String) = {
    throw new ParserException(s"Error[${line}:${column}]: ${msg}")
  }

  private def scan: Option[StringInterpolationToken] = {
    val buffer = new ArrayBuffer[Char]()
    var ch = peek
    var t = "normal"

    ch match {
      case _ if isExpression(ch, 1) =>
        val tempSrcPos = srcPos
        ch = scanExpression

        if (ch != '}') {
          error("Expression should be enclosed by '}'")
        }

        if (srcPos - tempSrcPos == 1) {
          error("start with :{ should be expression")
        }

        (tempSrcPos + 1 to srcPos).map { index =>
          buffer += srcChars(index)
        }
        t = "evaluate"
      case Scanner.EOF_INT =>
        return None
      case s: Char =>
        buffer += s
        next
    }
    Option(StringInterpolationToken(t, buffer.toArray))
  }

  private def isExpression(ch: Char, n: Int): Boolean = {
    if (n == 1) {
      val cont = (ch == ':' && peek2 == '{')
      return cont
    }
    if (ch == '}') {
      next
      return false
    }
    true
  }

  private def scanExpression = {
    var i = 1
    while (isExpression(peek, i)) {
      i += 1
      next
    }
    srcChars(srcPos)
  }

  private def peek2: Char = {
    if (srcPos + 2 >= srcEnd) return Scanner.EOF_INT
    val nextChar = srcChars(srcPos + 2)
    nextChar
  }

  private def peek: Char = {
    if (srcPos + 1 >= srcEnd) return Scanner.EOF_INT
    val nextChar = srcChars(srcPos + 1)
    nextChar
  }

  private def next: Char = {
    if (srcPos + 1 >= srcEnd) return Scanner.EOF_INT
    val nextChar = srcChars(srcPos + 1)
    srcPos += 1

    nextChar match {
      case '\n' =>
        line += 1
        column = 0
      case _ =>
        column += 1
    }

    nextChar
  }

}

