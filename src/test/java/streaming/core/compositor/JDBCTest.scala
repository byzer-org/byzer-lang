package streaming.core.compositor

import java.sql.DriverManager

import java.sql.Connection

/**
  * Created by allwefantasy on 20/2/2017.
  */
object ScalaJdbcConnectSelect {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:hive2://localhost:10000/default"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val start = System.currentTimeMillis()
      val resultSet = statement.executeQuery("SELECT * FROM test_table ")
      while ( resultSet.next() ) {
        println(" city = "+ resultSet.getString("city") )
      }
      println(System.currentTimeMillis() - start)
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

}
