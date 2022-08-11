package tech.mlsql.ets

import org.apache.hadoop.util.ToolRunner
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.core.datasource.{FSConfig, RewritableFSConfig}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.hdfs.{FSGetmerge, WowFsShell}
import tech.mlsql.runtime.AppRuntimeStore

/**
 * 2019-05-07 WilliamZhu(allwefantasy@gmail.com)
 */
class HDFSCommand(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    val fsConf = configRewrite(AppRuntimeStore.FS_BEFORE_CONFIG_KEY,
      FSConfig(df.sparkSession.sessionState.newHadoopConf(), path, params), ScriptSQLExec.context())
    val args = JSONTool.parseJson[List[String]](params("parameters"))
    fsConf.conf.setQuietMode(false)
    import spark.implicits._

    args.headOption match {
      case Some("utils") =>

        args.drop(1).headOption match {
          case Some("getmerge") =>
            // !fs utils getmerge "/tmp/*.csv" "/tmp/a.csv" 1;
            // !fs utils getmerge _  -source "/tmp/*.csv"  -target "/tmp/a.csv"  -skipNLines 1;
            val getmerge = new FSGetmerge(fsConf, args.drop(2))
            val (err, msg) = getmerge.run
            spark.createDataset[String](Seq(msg)).toDF("message")
          case _ =>
            spark.createDataset[String](Seq(s"Error: unknown utils command")).toDF("message")
        }

      case _ =>
        var output = ""
        val fsShell = new WowFsShell(fsConf.conf, fsConf.path)
        try {
          ToolRunner.run(fsShell, args.toArray)
          output = fsShell.getError

          if (output == null || output.isEmpty) {
            output = fsShell.getOut
          }
        }
        finally {
          fsShell.close()
        }
        if (args.contains("-F")) {
          val ds = spark.createDataset(output.split("\n").toSeq)
          spark.read.json(ds)
        } else {
          spark.createDataset[String](Seq(output)).toDF("fileSystem")
        }
    }
  }

  def configRewrite(orderKey: String,
                    config: FSConfig,
                    context: MLSQLExecuteContext): FSConfig = {
    AppRuntimeStore.store.getLoadSave(orderKey) match {
      case Some(item) =>
        item.customClassItems.classNames.map { className =>
          val instance = Class.forName(className).newInstance().asInstanceOf[RewritableFSConfig]
          instance.rewrite(config, context)
        }.headOption.getOrElse(config)
      case None =>
        config
    }
  }


  override def skipPathPrefix: Boolean = false

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new MLSQLException(s"${getClass.getName} not support register ")
  }
}
