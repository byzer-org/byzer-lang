package streaming.core.datasource

import java.sql.ResultSet

import org.apache.spark.sql.mlsql.session.MLSQLException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 2018-12-21 WilliamZhu(allwefantasy@gmail.com)
  */
object JDBCUtils {
  def getRsCloumns(rs: ResultSet): Array[String] = {
    val rsm = rs.getMetaData
    (0 until rsm.getColumnCount).map { index =>
      rsm.getColumnLabel(index + 1)
    }.toArray
  }

  def rsToMaps(rs: ResultSet): Seq[Map[String, Any]] = {
    val buffer = new ArrayBuffer[Map[String, Any]]()
    while (rs.next()) {
      buffer += rsToMap(rs, getRsCloumns(rs))
    }
    buffer
  }

  def rsToMap(rs: ResultSet, columns: Array[String]): Map[String, Any] = {
    val item = new mutable.HashMap[String, Any]()
    columns.foreach { col =>
      item.put(col, rs.getObject(col))
    }
    item.toMap
  }

  def executeQueryInDriver(options: Map[String, String]) = {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
    try {
      options.get("driver-statement-query").map { sql =>
        val stat = connection.prepareStatement(sql)
        val rs = stat.executeQuery()
        val res = JDBCUtils.rsToMaps(rs)
        stat.close()
        res
      }.getOrElse {
        throw new MLSQLException("driver-statement-query is required")
      }
    } finally {
      if (connection != null)
        connection.close()
    }

  }
}
