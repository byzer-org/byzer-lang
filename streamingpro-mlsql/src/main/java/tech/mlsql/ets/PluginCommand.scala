package tech.mlsql.ets


import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.nio.file.Files

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

    require(isDeltaLakeEnable(), "-streaming.datalake.path is required ")
    command match {
      case Seq(pluginType, "add", className, pluginName, left@_*) =>

        require(pluginType == PluginType.DS || pluginType == PluginType.ET, "pluginType should be ds or et")

        def isEt = {
          pluginType == PluginType.ET
        }

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

        loadJarInDriver(localPath)
        spark.sparkContext.addJar(localPath)


        checkVersionCompatibility(pluginName, className)

        val commandName = left.toArray match {
          case Array("named", commandName) =>
            Option(commandName)
          case _ => None
        }

        if (isEt) {
          registerET(pluginName, className, commandName, () => {
          })
          val etName = className.split("\\.").last
          saveTable(spark, spark.createDataset(Seq(ETRecord(pluginName, commandName, etName, className))).toDF(), TABLE_ETRecord, None, false)
        } else {
          registerDS(pluginName, className, commandName, () => {
            saveTable(spark, spark.createDataset(Seq(DSRecord(pluginName, commandName, className))).toDF(), TABLE_DSRecord, None, false)
          })
        }

        saveTable(
          spark, spark.createDataset(Seq(AddPlugin(pluginName, pluginPath))).toDF(),
          TABLE_PLUGINS, None, false)

        readTable(spark, TABLE_PLUGINS)

    }

  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

case class AddPlugin(pluginName: String, path: String)

case class ETRecord(pluginName: String, commandName: Option[String], etName: String, className: String)

case class DSRecord(pluginName: String, shortFormat: Option[String], fullFormat: String)

object PluginType {
  val ET = "et"
  val DS = "ds"
}

object PluginCommand {
  val TABLE_ETRecord = "__mlsql__.etRecord"
  val TABLE_DSRecord = "__mlsql__.dsRecord"
  val TABLE_PLUGINS = "__mlsql__.plugins"
  val TABLE_FILES = "__mlsql__.files"

  def downloadJarFile(spark: SparkSession, jarPath: String) = {
    val response = Request.Post(s"http://store.mlsql.tech/api/repo/plugins/download").connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().add("name", jarPath).build(),
      Charset.forName("utf-8")).execute().returnResponse()

    if (response.getStatusLine.getStatusCode != 200) {
      throw new MLSQLException(s"Fail to download ${jarPath} from http://store.mlsql.tech/api/repo/plugins/download")
    }

    var fieldValue = response.getFirstHeader("Content-Disposition").getValue
    val inputStream = response.getEntity.getContent
    fieldValue = fieldValue.substring(fieldValue.indexOf("filename=") + 10, fieldValue.length() - 1);

    val dataLake = new DataLake(spark)

    val hdfsPath = PathFun(dataLake.identifyToPath(TABLE_FILES)).add("store").add("plugins")
    HDFSOperator.saveStream(hdfsPath.toPath, fieldValue, inputStream)
    (fieldValue, PathFun(hdfsPath.toPath).add(fieldValue).toPath)

  }

  def downloadFromHDFS(fileName: String, pluginPath: String) = {
    val inputStream = HDFSOperator.readAsInputStream(pluginPath)

    val tmpLocation = new File("./__mlsql__/store/plugins")
    if (!tmpLocation.exists()) {
      tmpLocation.mkdirs()
    }
    val jarFile = new File(PathFun(tmpLocation.getPath).add(fileName).toPath)
    if (!jarFile.exists()) {
      Files.copy(inputStream, jarFile.toPath)
    } else {
      inputStream.close()
    }

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

  def registerDS(pluginName: String, className: String, commandName: Option[String], callback: () => Unit) = {
    val dataSource = Class.forName(className).newInstance()
    if (dataSource.isInstanceOf[MLSQLRegistry]) {
      dataSource.asInstanceOf[MLSQLRegistry].register()
    }
    commandName match {
      case Some(alisName) =>

        callback()
      case None =>
    }
  }
}