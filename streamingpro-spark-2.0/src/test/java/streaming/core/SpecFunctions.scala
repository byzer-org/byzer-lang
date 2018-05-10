package streaming.core

import java.sql.DriverManager

import org.apache.http.HttpVersion
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import streaming.dsl.ScriptSQLExecListener

/**
  * Created by allwefantasy on 28/4/2018.
  */
trait SpecFunctions {
  def request(url: String, params: Map[String, String]) = {
    val form = Form.form()
    params.map(f => form.add(f._1, f._2))
    val res = Request.Post(url)
      .useExpectContinue()
      .version(HttpVersion.HTTP_1_1).bodyForm(form.build())
      .execute().returnResponse()
    if (res.getStatusLine.getStatusCode != 200) {
      null
    } else {
      new String(EntityUtils.toByteArray(res.getEntity))
    }
  }

  def createSSEL(implicit spark: SparkSession) = {
    new ScriptSQLExecListener(spark, "/tmp/william", Map())
  }

  def jdbc(ddlStr: String) = {
    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
      "root",
      "csdn.net")
    val stat = con.createStatement()
    stat.execute(ddlStr)
    stat.close()
    con.close()
  }

  def scriptStr(name: String) = {
    val file = s"/test/sql/${name}.sql"
    val stream = SpecFunctions.this.getClass.getResourceAsStream(file)
    if (stream == null) throw new RuntimeException(s"load file: ${file} failed,please chech the path")
    scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def loadStr(name: String) = {
    scala.io.Source.fromInputStream(SpecFunctions.this.getClass.getResourceAsStream(s"/test/sql/${name}")).getLines().mkString("\n")
  }
}
