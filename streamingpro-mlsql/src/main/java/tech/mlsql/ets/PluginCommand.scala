package tech.mlsql.ets


import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod
import tech.mlsql.dsl.includes.PluginIncludeSource
import tech.mlsql.runtime.PluginUtils
import tech.mlsql.runtime.plugins._
import tech.mlsql.store.DBStore


/**
 * 2019-09-11 WilliamZhu(allwefantasy@gmail.com)
 */
class PluginCommand(override val uid: String) extends SQLAlg with ETAuth with WowParams {
  def this() = this(BaseParams.randomUID())

  import tech.mlsql.runtime.PluginUtils._

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val command = JSONTool.parseJson[List[String]](params("parameters"))

    def isDeltaLakeEnable() = {
      val dataLake = new DataLake(spark)
      dataLake.isEnable
    }

    def fetchTable(tableName: String, callback: () => DataFrame) = {
      TryTool.tryOrElse {
        DBStore.store.readTable(spark, tableName)
      } {
        callback()
      }
    }

    require(isDeltaLakeEnable(), "-streaming.datalake.path is required ")
    command match {
      case Seq(pluginType, "remove", pluginName) =>
        pluginType match {
          case PluginType.ET =>

            val items = fetchTable(TABLE_ETRecord,
              () => spark.createDataset[ETRecord](Seq()).toDF())
              .where($"pluginName" === pluginName).as[ETRecord].collect()

            if (items.size == 0) {
              throw new RuntimeException(s"${pluginName} is not found")
            }
            val item = items.head

            removeET(pluginName, item.className, item.commandName, () => {
              DBStore.store.saveTable(spark, spark.createDataset(Seq(
                ETRecord(pluginName, item.commandName, item.etName, item.className, null))).toDF(),
                TABLE_ETRecord, Option("pluginName"), true)
            })

          case PluginType.DS =>

            val items = fetchTable(TABLE_DSRecord,
              () => spark.createDataset[DSRecord](Seq()).toDF())
              .where($"pluginName" === pluginName).as[DSRecord].collect()

            if (items.size == 0) {
              throw new RuntimeException(s"Plugin [${pluginName}] is not found")
            }
            val item = items.head

            removeDS(pluginName, item.fullFormat, item.shortFormat, () => {
              DBStore.store.saveTable(spark, spark.createDataset(
                Seq(DSRecord(pluginName, item.shortFormat, item.fullFormat, null))).toDF(),
                TABLE_DSRecord, Option("pluginName"), true)
            })

          case PluginType.SCRIPT =>
            PluginIncludeSource.unRegister(pluginName)

          case PluginType.APP =>
            DBStore.store.saveTable(spark, spark.createDataset(Seq(AppRecord(pluginName, "", Seq(), null))).toDF(),
              TABLE_APPRecord, Option("pluginName"), true)
        }

        DBStore.store.saveTable(spark, spark.createDataset(Seq(AddPlugin(pluginName, "", pluginType, null))).toDF(),
          TABLE_PLUGINS, Option("pluginName,pluginType"), true)
        fetchTable(TABLE_PLUGINS, () => spark.createDataset[AddPlugin](Seq()).toDF())


      case Seq(pluginType, "add", _className, _pluginName, left@_*) =>

        require(pluginType == PluginType.DS
          || pluginType == PluginType.ET
          || pluginType == PluginType.SCRIPT
          || pluginType == PluginType.APP, "pluginType should be ds or et or script or app")


        val (pluginName, pluginVersion) = PluginUtils.getPluginNameAndVersion(_pluginName)
        val plugin = PluginUtils.getPluginInfo(pluginName).filter(f => f.version == pluginVersion).head
        val className = if (_className == "-") {
          val config = JSONTool.parseJson[Map[String, String]](plugin.extraParams)
          config("mainClass")
        } else {
          _className
        }
        val table = try {
          DBStore.store.readTable(spark, TABLE_PLUGINS)
        } catch {
          case e: Exception =>
            spark.createDataset[AddPlugin](Seq()).toDF()

        }
        if (table.where($"pluginName" === pluginName).count() > 0) {
          throw new MLSQLException(s"${pluginName} is already installed.")
        }

        val (fileName, pluginPath) = downloadJarFileToHDFS(spark, pluginName, pluginVersion)

        val localPath = downloadFromHDFSToLocal(fileName, pluginPath,spark.sparkContext.hadoopConfiguration)

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
              val etName = className.split("\\.").last
              DBStore.store.saveTable(spark, spark.createDataset(
                Seq(ETRecord(pluginName, commandName, etName, className, pluginVersion))).toDF(),
                TABLE_ETRecord, None, false)
            })

          case PluginType.DS =>
            registerDS(pluginName, className, commandName, () => {
              DBStore.store.saveTable(spark, spark.createDataset(
                Seq(DSRecord(pluginName, commandName, className, pluginVersion))).toDF(),
                TABLE_DSRecord, None, false)
            })
          case PluginType.SCRIPT =>
            PluginIncludeSource.register(pluginName, localPath)

          case PluginType.APP =>
            DBStore.store.saveTable(spark,
              spark.createDataset(Seq(AppRecord(pluginName, className, left, pluginVersion))).toDF(),
              TABLE_APPRecord, None, false)
        }


        DBStore.store.saveTable(
          spark, spark.createDataset(Seq(AddPlugin(pluginName, pluginPath, pluginType, pluginVersion))).toDF(),
          TABLE_PLUGINS, None, false)

        DBStore.store.readTable(spark, TABLE_PLUGINS)

      case Seq("script", "show", item) =>
        val includeSource = new PluginIncludeSource()
        val content = includeSource.fetchSource(spark, item, Map[String, String]())
        spark.createDataset[ScriptContent](Seq(ScriptContent(item, content))).toDF()

      case Seq("list") => DBStore.store.readTable(spark, TABLE_PLUGINS)

      case Seq("list", pluginType) =>
        DBStore.store.readTable(spark, TABLE_PLUGINS).where(s""" pluginType="${pluginType}" """)

    }


  }

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__plugin_operator__"),
      OperateType.INSERT,
      Option("_mlsql_"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None => List(TableAuthResult(true, ""))
    }
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???


}


case class ScriptContent(path: String, value: String)