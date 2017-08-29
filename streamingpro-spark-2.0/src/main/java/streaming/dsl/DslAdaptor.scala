package streaming.dsl

import streaming.dsl.parser.DSLSQLParser.SqlContext

/**
  * Created by allwefantasy on 27/8/2017.
  */
trait DslAdaptor {
  def parse(ctx: SqlContext): Unit

  def cleanStr(str: String) = {
    if(str.startsWith("`")||str.startsWith("\""))
    str.substring(1, str.length - 1)
    else str
  }
}
