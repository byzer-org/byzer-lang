package tech.mlsql.ets


import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.Files

import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
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
      case Seq("add", className, pluginName, left@_*) =>

        val table = try {
          readTable(spark, "__mlsql__.plugins")
        } catch {
          case e: Exception =>
            spark.createDataset[AddPlugin](Seq()).toDF()

        }
        if (table.where($"name" === pluginName).count() > 0) {
          throw new MLSQLException(s"${pluginName} is already installed.")
        }

        val (fileName, pluginPath) = downloadJarFile(pluginName)
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

        loadJarInDriver(localPath)
        spark.sparkContext.addJar(localPath)
        val etName = className.split("\\.").last
        ETRegister.register(etName, className)

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

        left.toArray match {
          case Array("named", commandName) =>
            CommandCollection.refreshCommandMapping(Map(commandName -> etName))
            saveTable(spark, spark.createDataset(Seq(ETCommand(pluginName, etName))).toDF(), "__mlsql__.etCommand", None, false)
          case _ =>
        }

        saveTable(
          spark, spark.createDataset(Seq(AddPlugin(pluginName, pluginPath))).toDF(),
          "__mlsql__.plugins", None, false)

        readTable(spark, "__mlsql__.plugins")

    }

  }


  def downloadJarFile(jarPath: String) = {
    val response = Request.Post(s"http://store.mlsql.tech/repo/plugins/download").connectTimeout(60 * 1000)
      .socketTimeout(60 * 60 * 1000).bodyForm(Form.form().add("name", jarPath).build()).execute().returnResponse()
    var fieldValue = response.getFirstHeader("Content-Disposition").getValue
    val inputStream = response.getEntity.getContent
    fieldValue = fieldValue.substring(fieldValue.indexOf("filename=\"") + 10, fieldValue.length() - 1);
    //    val inputStream = HDFSOperator.readAsInputStream("/Users/allwefantasy/CSDNWorkSpace/deltaehancer/target/delta-ehancer-0.1.0-SNAPSHOT.jar")
    //    val fieldValue = "delta-ehancer-0.1.0-SNAPSHOT.jar"

    val context = ScriptSQLExec.context()
    val hdfsPath = PathFun(context.home).add("__mlsql__").add("store").add("plugins")
    HDFSOperator.saveStream(hdfsPath.toPath, fieldValue, inputStream)
    (fieldValue, PathFun(hdfsPath.toPath).add(fieldValue).toPath)

  }

  def loadJarInDriver(path: String) = {

    val systemClassLoader = ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader]
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    method.invoke(systemClassLoader, new File(path).toURI.toURL)
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???
}

case class AddPlugin(name: String, path: String)

case class ETCommand(name: String, etName: String)