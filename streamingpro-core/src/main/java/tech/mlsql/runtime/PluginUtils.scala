package tech.mlsql.runtime

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardCopyOption}

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.core.datasource.MLSQLRegistry
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.core.version.MLSQLVersion
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility

/**
 * 27/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object PluginUtils {
  val TABLE_ETRecord = "__mlsql__.etRecord"
  val TABLE_DSRecord = "__mlsql__.dsRecord"
  val TABLE_APPRecord = "__mlsql__.appRecord"
  val TABLE_PLUGINS = "__mlsql__.plugins"
  val TABLE_FILES = "__mlsql__.files"

  def downloadJarFile(spark: SparkSession, jarPath: String) = {
    val response = Request.Post(s"http://store.mlsql.tech/api/repo/plugins/download").connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().add("name", jarPath).build(),
      Charset.forName("utf-8")).execute().returnResponse()

    if (response.getStatusLine.getStatusCode != 200 || response.getFirstHeader("Content-Disposition") == null) {
      throw new MLSQLException(s"Fail to download ${jarPath} from http://store.mlsql.tech/api/repo/plugins/download")
    }

    var fieldValue = response.getFirstHeader("Content-Disposition").getValue
    val inputStream = response.getEntity.getContent
    fieldValue = fieldValue.substring(fieldValue.indexOf("filename=") + 10, fieldValue.length() - 1);

    val dataLake = new DataLake(spark)

    val hdfsPath = PathFun(dataLake.identifyToPath(TABLE_FILES)).add("store").add("plugins")
    HDFSOperator.saveStream(hdfsPath.toPath, fieldValue, inputStream)
    HDFSOperator.deleteDir("." + hdfsPath.toPath + ".crc")
    (fieldValue, PathFun(hdfsPath.toPath).add(fieldValue).toPath)

  }

  def downloadFromHDFS(fileName: String, pluginPath: String) = {
    val inputStream = HDFSOperator.readAsInputStream(pluginPath)

    val tmpLocation = new File("./__mlsql__/store/plugins")
    if (!tmpLocation.exists()) {
      tmpLocation.mkdirs()
    }
    val jarFile = new File(PathFun(tmpLocation.getPath).add(fileName).toPath)
    if (jarFile.exists()) {
      jarFile.delete()
    }
    Files.copy(inputStream, jarFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    inputStream.close()
    val localPath = jarFile.getPath
    localPath
  }

  def loadJarInDriver(path: String) = {

    val systemClassLoader = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(systemClassLoader, new File(path).toURI.toURL)
  }

  def checkVersionCompatibility(pluginName: String, className: String) = {
    val versions = Class.forName(className).newInstance().asInstanceOf[VersionCompatibility].supportedVersions
    if (!versions.contains(MLSQLVersion.version().version)) {
      throw new MLSQLException(
        s"""
           |Plugins ${pluginName} supports:
           |
           |${versions.mkString(",")}
           |
           |Current MLSQL Engine version: ${MLSQLVersion.version().version}
            """.stripMargin)
    }
  }


  def appCallBack(pluginName: String, className: String, params: Seq[String]) = {
    val app = Class.forName(className).newInstance().asInstanceOf[tech.mlsql.app.App]
    app.run(params)
  }

  def registerET(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val etName = className.split("\\.").last
    ETRegister.register(etName, className)
    commandName match {
      case Some(alisName) =>
        CommandCollection.refreshCommandMapping(Map(alisName -> etName))
        callback()
      case None =>
    }


  }

  def removeET(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val etName = className.split("\\.").last
    ETRegister.remove(etName)
    commandName match {
      case Some(alisName) =>
        CommandCollection.remove(alisName)
        callback()
      case None =>
    }

  }

  def removeDS(pluginName: String, fullFormat: String, shortFormat: Option[String], callback: () => Unit) = {
    val dataSource = Class.forName(fullFormat).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].unRegister()
    }
    callback()
  }

  def registerDS(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val dataSource = Class.forName(className).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
    callback()
  }
}
