package streaming.core.strategy.platform

import org.apache.spark.sql.jdbc.JdbcDialect

/**
  * @Author: Alan
  * @Time: 2018/12/18 17:43
  * @Description:
  */
private case object HiveJdbcDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = url.startsWith("jdbc:hive2")
  override def quoteIdentifier(colName: String): String = {
    val col =parseTableAndColFromStr(colName)
    s"`$col`"
  }
  def parseTableAndColFromStr(str: String) = {
    var cleanedStr = ""
    if (str.startsWith("`") || str.startsWith("\""))
      cleanedStr = str.substring(1, str.length - 1)
    else
      cleanedStr = str
    val tableAndCol = cleanedStr.split("\\.")
    if (tableAndCol.length > 1) {
      val table = tableAndCol(0)
      val  col = tableAndCol.splitAt(1)._2.mkString(".")
      col
    } else {
      cleanedStr
    }

  }
}
