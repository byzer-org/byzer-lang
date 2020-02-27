package streaming.core.datasource

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.Files

import org.apache.http.client.fluent.{Form, Request}
import streaming.dsl.ScriptSQLExec
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool

/**
  * 2019-01-14 WilliamZhu(allwefantasy@gmail.com)
  */
class DataSourceRepository(url: String) extends Logging with WowLog {


  //"http://respository.datasource.mlsql.tech"
  def getOrDefaultUrl = {
    if (url == null || url.isEmpty) {
      val context = ScriptSQLExec.contextGetOrForTest()
      context.userDefinedParam.getOrElse("__datasource_repository_url__", "http://datasource.repository.mlsql.tech")
    } else {
      url
    }
  }

  def versionCommand(name: String, sparkV: String) = {
    val res = Request.Post(s"${getOrDefaultUrl}/repo/versions").connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).bodyForm(Form.form().add("name", name).
      add("scalaV", "2.11").add("sparkV", sparkV).build()
    ).execute().returnContent().asString()
    JSONTool.parseJson[List[String]](res)
  }

  def list() = {
    val res = Request.Get(s"${getOrDefaultUrl}/repo/list").connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).execute().returnContent().asString()
    JSONTool.parseJson[List[String]](res)
  }

  def addCommand(name: String, version: String, sparkV: String, scalaV: String = "2.11") = {

    // fileName format e.g es, mongodb
    logInfo(s"downloading ${name} from ${getOrDefaultUrl}")
    val response = Request.Post(s"${getOrDefaultUrl}/repo/download").connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000).bodyForm(Form.form().add("name", name).
      add("scalaV", scalaV).add("sparkV", sparkV).add("version", version).build()).execute().returnResponse()
    var fieldValue = response.getFirstHeader("Content-Disposition").getValue
    val inputStream = response.getEntity.getContent
    fieldValue = fieldValue.substring(fieldValue.indexOf("filename=\"") + 10, fieldValue.length() - 1);
    val tmpLocation = new File("./dataousrce_upjars")
    if (!tmpLocation.exists()) {
      tmpLocation.mkdirs()
    }
    val jarFile = new File(PathFun(tmpLocation.getPath).add(fieldValue).toPath)
    if (!jarFile.exists()) {
      Files.copy(inputStream, jarFile.toPath)
    } else {
      inputStream.close()
    }
    jarFile.getPath
  }

  def loadJarInDriver(path: String) = {

    val systemClassLoader = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(systemClassLoader, new File(path).toURI.toURL)
  }

}
