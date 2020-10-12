package tech.mlsql.lang.cmd.compile.internal.gc

import scala.collection.mutable.ArrayBuffer


class TextTemplate(template: Map[String, Any], str: String) {
  var srcPos = -1
  val srcEnd = str.length
  val srcChars = str.toCharArray

  def parse = {
    val buffer = new ArrayBuffer[Char]()
    var wow = scan
    while (!wow.isEmpty) {
      buffer ++= wow
      wow = scan
    }
    buffer.mkString("")
  }

  private def scan: ArrayBuffer[Char] = {
    val buffer = new ArrayBuffer[Char]()
    var ch = peek
    while (whiteSpace(peek)) {
      ch = next
      buffer += ch
    }
    ch = peek

    ch match {
      case s if isVariable(ch, 1) =>
        val tempSrcPos = srcPos
        ch = scanVariable
        if (srcPos - tempSrcPos == 1 || srcPos - tempSrcPos > 256) {
          (tempSrcPos+1 to srcPos).map { index =>
            buffer += srcChars(index)
          }
        } else {
          val name = ((tempSrcPos+1) to srcPos).map(srcChars(_)).mkString("")
          val newname = name.substring(1, name.length)
          if (template.contains(newname)) {
            buffer ++= template(newname).toString.toCharArray
          } else {
            ((tempSrcPos+1) to srcPos).map { index =>
              buffer += srcChars(index)
            }
          }
        }
      case Scanner.EOF_INT => return buffer
      case s: Char =>
        buffer += s
        next

    }
    return buffer
  }

  private def scanVariable = {
    var i = 1
    while (isVariable(peek, i)) {
      i += 1
      next
    }
    srcChars(srcPos)
  }

  private def isVariable(ch: Char, n: Int): Boolean = {
    if (n == 1) {
      return ch == ':'
    }
    ch.isLetter || ch.isDigit || ch == '_'
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
    nextChar
  }

  private def whiteSpace(s: Char) = {
    s == '\t' || s == '\n' || s == '\r' || s == ' ' || s == ';'
  }

}

