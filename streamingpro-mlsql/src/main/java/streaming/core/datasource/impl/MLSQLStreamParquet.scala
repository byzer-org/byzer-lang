package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLStreamParquet(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val streamReader = config.df.get.sparkSession.readStream
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", "parquet")
    val owner = config.config.get("owner").getOrElse(context.owner)
    streamReader.options(rewriteConfig(config.config)).format(format).load(resolvePath(config.path, owner))
  }

  override def fullFormat: String = "org.apache.spark.sql.execution.streaming.newfile"

  override def shortFormat: String = "streamParquet"

  override def resolvePath(path: String, owner: String): String = {
    val context = ScriptSQLExec.contextGetOrForTest()
    resourceRealPath(context.execListener, Option(context.owner), path)
  }
}
