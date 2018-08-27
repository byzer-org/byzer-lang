package streaming.dsl

import org.antlr.v4.runtime.misc.Interval
import streaming.dsl.parser.DSLSQLLexer
import streaming.dsl.parser.DSLSQLParser.SqlContext

/**
  * Created by allwefantasy on 27/8/2017.
  */
trait DslAdaptor extends DslTool {
  def parse(ctx: SqlContext): Unit
}

trait DslTool {

  def currentText(ctx: SqlContext) = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    input.getText(interval)
  }

  def cleanStr(str: String) = {
    if (str.startsWith("`") || str.startsWith("\""))
      str.substring(1, str.length - 1)
    else str
  }

  def cleanBlockStr(str: String) = {
    if (str.startsWith("'''") && str.endsWith("'''"))
      str.substring(3, str.length - 3)
    else str
  }

  def withPathPrefix(prefix: String, path: String): String = {

    val newPath = cleanStr(path)
    if (prefix.isEmpty) return newPath

    if (path.contains("..")) {
      throw new RuntimeException("path should not contains ..")
    }
    if (path.startsWith("/")) {
      return prefix + path.substring(1, path.length)
    }
    return prefix + newPath

  }

  def parseDBAndTableFromStr(str: String) = {
    val dbAndTable = cleanStr(str).split("\\.")
    if (dbAndTable.length > 1) {
      val db = dbAndTable(0)
      val table = dbAndTable.splitAt(1)._2.mkString(".")
      (db, table)
    } else {
      (str, str)
    }

  }
}
