package tech.mlsql.runtime

import net.csdn.common.reflect.ReflectHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.http.HttpResponse
import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.SparkCoreVersion
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.core.datasource.MLSQLRegistry
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.log.WowLog
import tech.mlsql.common.utils.classloader.ClassLoaderTool
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.core.version.MLSQLVersion
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.store.{DBStore, DictType}
import tech.mlsql.tool.HDFSOperatorV2
import tech.mlsql.version.VersionCompatibility

import java.io.{File, InputStream, OutputStream}
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardCopyOption}

/**
 * 27/2/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object PluginUtils extends Logging with WowLog {
  val TABLE_ETRecord = "__mlsql__.etRecord"
  val TABLE_DSRecord = "__mlsql__.dsRecord"
  val TABLE_APPRecord = "__mlsql__.appRecord"
  val TABLE_PLUGINS = "__mlsql__.plugins"
  val TABLE_FILES = "__mlsql__.files"

  def runtime = PlatformManager.getRuntime

  def PLUGIN_STORE_URL = {
    DBStore.store.readConfig(runtime.asInstanceOf[SparkRuntime].sparkSession, "", "proxy", DictType.MLSQL_CONFIG) match {
      case Some(item) =>
        item.value
      case None => "http://store.mlsql.tech/run"
    }
  }

  def getPluginInfo(name: String) = {
    val pluginListResponse = Request.Post(PLUGIN_STORE_URL).connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().
      add("action", "getPlugin").
      add("pluginName", name).
      add("pluginType", "MLSQL_PLUGIN").
      build(),
      Charset.forName("utf-8")).execute().returnContent().asString(Charset.forName("utf-8"))
    JSONTool.parseJson[List[PluginStoreItem]](pluginListResponse)
  }

  def getLatestPluginInfo(name: String) = {
    val plugins = getPluginInfo(name)
    if (plugins.size == 0) {
      throw new MLSQLException(s"Plugin name:${name}  is not found in market")
    }
    plugins.sortBy(_.version).last
  }

  def getPluginNameAndVersion(name: String): (String, String) = {
    if (name.contains(":") || name.contains("==")) {
      name.split(":|==") match {
        case Array(name, version) => (name, version)
      }
    } else {
      (name, getLatestPluginInfo(name).version)
    }
  }

  def downloadJarFileToHDFS(spark: SparkSession, pluginName: String, version: String) = {


    val wrapperResponse = Request.Post(PLUGIN_STORE_URL).connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().
      add("action", "downloadPlugin").
      add("pluginName", pluginName).
      add("pluginType", "MLSQL_PLUGIN").
      add("version", version).
      build(),
      Charset.forName("utf-8")).execute()
    val response = ReflectHelper.field(wrapperResponse, "response").asInstanceOf[HttpResponse]

    if (response.getStatusLine.getStatusCode != 200 || response.getFirstHeader("Content-Disposition") == null) {
      throw new MLSQLException(s"Fail to download ${pluginName} from ${PLUGIN_STORE_URL}")
    }


    var fieldValue = response.getFirstHeader("Content-Disposition").getValue
    val fileLenHeader = response.getFirstHeader("Content-Length")
    val fileLen = if (fileLenHeader != null) {
      fileLenHeader.getValue.toLong
    } else -1


    val inputStream = response.getEntity.getContent
    fieldValue = fieldValue.substring(fieldValue.indexOf("filename=") + 10, fieldValue.length() - 1);

    val dataLake = new DataLake(spark)

    val hdfsPath = PathFun(dataLake.identifyToPath(TABLE_FILES)).add("store").add("plugins")
    logInfo(format(s"Download ${pluginName} to ${hdfsPath.toPath}"))
    saveStream(pluginName, fileLen, hdfsPath.toPath, fieldValue, inputStream, spark.sparkContext.hadoopConfiguration)
    HDFSOperatorV2.deleteDir("." + hdfsPath.toPath + ".crc")
    (fieldValue, PathFun(hdfsPath.toPath).add(fieldValue).toPath)

  }

  def saveStream(pluginName: String, fileLen: Long, path: String, fileName: String, inputStream: InputStream, hadoopConf: Configuration) = {

    def formatNumber(wow: Double): String = {
      if (wow == -1) return "UNKNOW"
      "%1.2f".format(wow)
    }

    def toKBOrMBStr(totalRead: Double): String = {
      if (totalRead / 1024 / 1024 > 1) s"${formatNumber(totalRead / 1024 / 1024)}MB"
      else s"${formatNumber(totalRead / 1024)}KB"
    }

    def KB(bytes: Double) = {
      bytes / 1024
    }

    def MB(bytes: Double) = {
      bytes / 1024 / 1024
    }

    def copyBytes(in: InputStream, out: OutputStream, buffSize: Int) = {
      val buf = new Array[Byte](buffSize);
      var bytesRead = in.read(buf)
      var totalRead = 0D
      var showProgressSize = 0L
      var logByteInterval = 100 //KB
      while (bytesRead >= 0) {
        out.write(buf, 0, bytesRead);
        totalRead += bytesRead
        showProgressSize += bytesRead
        if (KB(showProgressSize) > logByteInterval) {
          val progress = if (fileLen == -1) -1 else totalRead / fileLen

          logInfo(format(s"Downloading plugin ${pluginName}. " +
            s"Progress: ${formatNumber(progress * 100)}% / Download:${toKBOrMBStr(totalRead)}/${toKBOrMBStr(fileLen)}"))
          showProgressSize = 0L
          if (MB(totalRead) > 5) {
            logByteInterval = 500 //KB
          }
        }
        bytesRead = in.read(buf);
      }
      logInfo(format(s"Plugin ${pluginName} have been downloaded. size:${totalRead / 1024}KB "))
    }

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(hadoopConf)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      copyBytes(inputStream, dos, 4 * 1024 * 1024)
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }

  def readAsInputStream(fileName: String, conf: Configuration): InputStream = {
    val fs = FileSystem.get(conf)
    val src: Path = new Path(fileName)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
    } catch {
      case e: Exception =>
        if (in != null) in.close()
    }
    return in
  }

  def downloadFromHDFSToLocal(fileName: String, pluginPath: String, conf: Configuration) = {
    val inputStream = readAsInputStream(pluginPath, conf)

    val tmpLocation = new File(PathFun.current.add("store").add("plugins").toPath)
    if (!tmpLocation.exists()) {
      tmpLocation.mkdirs()
    }
    val jarFile = new File(PathFun(tmpLocation.getPath).add(fileName).toPath)
    if (jarFile.exists()) {
      jarFile.delete()
    }
    logInfo(format(s"Download [local]${jarFile.toPath} from [remote]${pluginPath}"))

    Files.copy(inputStream, jarFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    inputStream.close()
    val localPath = jarFile.getPath
    localPath
  }

  def loadJarInDriver(path: String) = {
    //.getSystemClassLoader()
    val systemClassLoader = ClassLoaderTool.getContextOrDefaultLoader
    if (systemClassLoader.isInstanceOf[URLClassLoader]) {
      val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
      method.setAccessible(true)
      method.invoke(systemClassLoader, new File(path).toURI.toURL)
    } else {

      try {
        val method = systemClassLoader.getClass()
          .getDeclaredMethod("appendToClassPathForInstrumentation", classOf[String])
        method.setAccessible(true)
        method.invoke(systemClassLoader, path)
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            s"""
               |In order to dynamically install plugin in JDK[${System.getProperty("java.version")}] > 8
               |you may need to add following to VM options:
               |
               |--add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens jdk.zipfs/jdk.nio.zipfs=ALL-UNNAMED
               |
               |""".stripMargin)
      }

    }

  }

  def checkVersionCompatibility(pluginName: String, className: String) = {
    val versionPatterns = ClassLoaderTool.classForName(className).newInstance().asInstanceOf[VersionCompatibility].supportedVersions
    val mlsqlVersion = MLSQLVersion.version().version
    val sparkVersion = SparkCoreVersion.exactVersion
    var compatible: Boolean = false

    versionPatterns.foreach(versionPattern => {
      if (!compatible) {
        compatible = VersionRangeChecker.isComposedVersionCompatible(versionPattern, mlsqlVersion, sparkVersion)
      }
    })

    if (!compatible) {
      throw new MLSQLException(
        s"""
           |Plugins ${pluginName} supports:
           |
           |${versionPatterns.mkString(",")}
           |
           |Current MLSQL Engine version: ${MLSQLVersion.version().version}
            """.stripMargin)
    }
  }


  def appCallBack(pluginName: String, className: String, params: Seq[String]) = {
    logInfo(format(s"register ${className} for ${pluginName}  with app callback"))
    val app = ClassLoaderTool.classForName(className).newInstance().asInstanceOf[tech.mlsql.app.App]
    app.run(params)
  }

  def registerET(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val etName = className.split("\\.").last

    logInfo(format(s"register ${className}/${etName} for ${pluginName}  with et callback"))
    ETRegister.register(etName, className)
    commandName match {
      case Some(alisName) =>
        CommandCollection.refreshCommandMapping(Map(alisName -> etName))

      case None =>
    }
    callback()
  }

  def removeET(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val etName = className.split("\\.").last
    ETRegister.remove(etName)
    commandName match {
      case Some(alisName) =>
        CommandCollection.remove(alisName)
      case None =>
    }
    callback()
  }

  def removeDS(pluginName: String, fullFormat: String, shortFormat: Option[String], callback: () => Unit) = {
    val dataSource = ClassLoaderTool.classForName(fullFormat).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].unRegister()
    }
    callback()
  }

  def registerDS(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    logInfo(format(s"register ${className} for ${pluginName}  with ds callback"))
    val dataSource = ClassLoaderTool.classForName(className).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
    callback()
  }
}

case class PluginStoreItem(id: Int, name: String, path: String, version: String, pluginType: Int, extraParams: String)


