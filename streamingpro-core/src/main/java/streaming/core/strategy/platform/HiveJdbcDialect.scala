package streaming.core.strategy.platform

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, StringType}

/**
 * @Author: Alan
 * @Time: 2018/12/18 17:43
 * @Description:
 */
private case object HiveJdbcDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = {
    var cleanedStr = ""
    if (colName.startsWith("`") || colName.startsWith("\""))
      cleanedStr = colName.replace("`", "").replace("\"", "")
    else
      cleanedStr = colName
    val tableAndCol = cleanedStr.split("\\.")
    if (tableAndCol.length > 1) {
      tableAndCol.map(part => s"`$part`").mkString(".")
    } else {
      s"`$cleanedStr`"
    }
  }

  /**
   * Adapt to Hive data type definitions
   * in https://cwiki.apache.org/confluence/display/hive/languagemanual+types .
   *
   * @param dt DataType in Spark SQL
   * @return JdbcType with type definition adapted to Hive
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // [HIVE-14950] "INTEGER" is synonym for INT since Hive 2.2.0
    // fallback to "INT" for better compatibility
    case IntegerType => Option(JdbcType("INT", java.sql.Types.INTEGER))
    // [HIVE-13556] "DOUBLE PRECISION" is alias for "DOUBLE" since Hive 2.2.0
    // fallback to "DOUBLE" for better compatibility
    case DoubleType => Option(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
    // adapt to Hive data type definition
    case FloatType => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case ByteType => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BIT))
    case StringType => Option(JdbcType("STRING", java.sql.Types.CLOB))
    case BinaryType => Option(JdbcType("BINARY", java.sql.Types.BLOB))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

}
