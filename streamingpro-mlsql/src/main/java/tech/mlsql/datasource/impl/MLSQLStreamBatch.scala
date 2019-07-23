package tech.mlsql.datasource.impl

import org.apache.spark.sql.streaming.{DataStreamWriter, MLSQLForeachBatchRunner}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.ets.ScriptRunner

/**
  * 2019-05-26 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLStreamBatch(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException(s"format ${shortFormat} not support load")
  }


  override def foreachBatchCallback(dataStreamWriter: DataStreamWriter[Row], options: Map[String, String]): Unit = {
    if (options.contains("code") && options.contains("sourceTable")) {
      val context = ScriptSQLExec.contextGetOrForTest()
      MLSQLForeachBatchRunner.run(dataStreamWriter, options("sourceTable"), (batchId: Long, sparkSessionForStream: SparkSession) => {
        ScriptSQLExec.setContext(context)
        ScriptRunner.rubSubJob(options("code"), (df) => {}, Option(sparkSessionForStream), false, false)
      })
    }
  }

  override def fullFormat: String = "custom"

  override def shortFormat: String = "custom"

  override def resolvePath(path: String, owner: String): String = {
    val context = ScriptSQLExec.contextGetOrForTest()
    resourceRealPath(context.execListener, Option(context.owner), path)
  }

  override def skipFormat: Boolean = true
}