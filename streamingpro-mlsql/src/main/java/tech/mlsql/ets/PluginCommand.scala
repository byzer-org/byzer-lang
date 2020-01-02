package tech.mlsql.ets


import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardCopyOption}

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.MLSQLRegistry
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.core.version.MLSQLVersion
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.CommandCollection
import tech.mlsql.dsl.includes.PluginIncludeSource
import tech.mlsql.ets.register.ETRegister
import tech.mlsql.version.VersionCompatibility


/**
 * 2019-09-11 WilliamZhu(allwefantasy@gmail.com)
 */
class PluginCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  import PluginCommand._
  import SchedulerCommand._

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val command = JSONTool.parseJson[List[String]](params("parameters"))

    def isDeltaLakeEnable() = {
      val dataLake = new DataLake(spark)
      dataLake.isEnable
    }

    def fetchTable(tableName: String, callback: () => DataFrame) = {
      val table = try {
        readTable(spark, tableName)
      } catch {
        case e: Exception =>
          callback()

      }
      table
    }

    require(isDeltaLakeEnable(), "-streaming.datalake.path is required ")
    command match {
      case Seq(pluginType, "remove", pluginName) =>
        pluginType match {
          case PluginType.ET =>
            val item = fetchTable(TABLE_ETRecord, () => spark.createDataset[ETRecord](Seq()).toDF()).where($"pluginName" === pluginName).as[ETRecord].head()
            removeET(pluginName, item.className, item.commandName, () => {
              saveTable(spark, spark.createDataset(Seq(ETRecord(pluginName, item.commandName, item.etName, item.className))).toDF(), TABLE_ETRecord, Option("pluginName"), true)
            })

          case PluginType.DS =>
            val item = fetchTable(TABLE_DSRecord, () => spark.createDataset[DSRecord](Seq()).toDF()).where($"pluginName" === pluginName).as[DSRecord].head()
            removeDS(pluginName, item.fullFormat, item.shortFormat, () => {
              saveTable(spark, spark.createDataset(Seq(DSRecord(pluginName, item.shortFormat, item.fullFormat))).toDF(), TABLE_DSRecord, Option("pluginName"), true)
            })
          case PluginType.SCRIPT =>
            PluginIncludeSource.unRegister(pluginName)

          case PluginType.APP =>
            saveTable(spark, spark.createDataset(Seq(AppRecord(pluginName, "", Seq()))).toDF(), TABLE_APPRecord, Option("pluginName"), true)
        }

        saveTable(spark, spark.createDataset(Seq(AddPlugin(pluginName, "", pluginType))).toDF(), TABLE_PLUGINS, Option("pluginName,pluginType"), true)
        readTable(spark, TABLE_PLUGINS)


      case Seq(pluginType, "add", className, pluginName, left@_*) =>

        require(pluginType == PluginType.DS
          || pluginType == PluginType.ET
          || pluginType == PluginType.SCRIPT
          || pluginType == PluginType.APP, "pluginType should be ds or et or script or app")

        val table = try {
          readTable(spark, TABLE_PLUGINS)
        } catch {
          case e: Exception =>
            spark.createDataset[AddPlugin](Seq()).toDF()

        }
        if (table.where($"pluginName" === pluginName).count() > 0) {
          throw new MLSQLException(s"${pluginName} is already installed.")
        }

        val (fileName, pluginPath) = downloadJarFile(spark, pluginName)

        val localPath = downloadFromHDFS(fileName, pluginPath)

        if (pluginType == PluginType.DS || pluginType == PluginType.ET || pluginType == PluginType.APP) {
          loadJarInDriver(localPath)
          spark.sparkContext.addJar(localPath)
          checkVersionCompatibility(pluginName, className)
        }


        val commandName = left.toArray match {
          case Array("named", commandName) =>
            Option(commandName)
          case _ => None
        }

        if (pluginType == PluginType.APP) {
          appCallBack(pluginName, className, left)
        }

        pluginType match {
          case PluginType.ET =>
            registerET(pluginName, className, commandName, () => {
            })
            val etName = className.split("\\.").last
            saveTable(spark, spark.createDataset(Seq(ETRecord(pluginName, commandName, etName, className))).toDF(), TABLE_ETRecord, None, false)
          case PluginType.DS =>
            registerDS(pluginName, className, commandName, () => {
              saveTable(spark, spark.createDataset(Seq(DSRecord(pluginName, commandName, className))).toDF(), TABLE_DSRecord, None, false)
            })
          case PluginType.SCRIPT =>
            PluginIncludeSource.register(pluginName, localPath)

          case PluginType.APP =>
            saveTable(spark, spark.createDataset(Seq(AppRecord(pluginName, className, left))).toDF(), TABLE_APPRecord, None, false)
        }


        saveTable(
          spark, spark.createDataset(Seq(AddPlugin(pluginName, pluginPath, pluginType))).toDF(),
          TABLE_PLUGINS, None, false)

        readTable(spark, TABLE_PLUGINS)

      case Seq("script", "show", item) =>
        val includeSource = new PluginIncludeSource()
        val content = includeSource.fetchSource(spark, item, Map[String, String]())
        spark.createDataset[ScriptContent](Seq(ScriptContent(item, content))).toDF()

    }


  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

case class AddPlugin(pluginName: String, path: String, pluginType: String)

case class ETRecord(pluginName: String, commandName: Option[String], etName: String, className: String)

case class DSRecord(pluginName: String, shortFormat: Option[String], fullFormat: String)

case class AppRecord(pluginName: String, className: String, params: Seq[String])


object PluginType {
  val ET = "et"
  val DS = "ds"
  val SCRIPT = "script"
  val APP = "app"
}

object PluginCommand {
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

  def removeDS(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val dataSource = Class.forName(className).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].unRegister()
    }
    commandName match {
      case Some(alisName) =>
        callback()
      case None =>
    }
  }

  def registerDS(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val dataSource = Class.forName(className).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
    callback()
  }
}

case class ScriptContent(path: String, value: String)