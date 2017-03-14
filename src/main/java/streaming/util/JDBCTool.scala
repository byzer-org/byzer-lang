package streaming.util

import java.sql.{Connection, DriverManager}
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 13/3/2017.
  */
object JDBCTool {
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = args(0)
    val url = args(1) //"jdbc:mysql://localhost:3306/alarm_test?user=root&password=csdn.net"//args(0)
    val sql = args(2)//"show tables;"//args(1)

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)
      println(JDBCHelper.getRsCloumns(resultSet).mkString("   "))
      JDBCHelper.rsToMaps(resultSet).foreach(f => println(f.map(k => k._2).mkString("    ")))

    } catch {
      case e:Exception => e.printStackTrace
    }
    connection.close()
  }

}
