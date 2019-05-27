package tech.mlsql.datasource.impl

import org.apache.spark.sql.streaming.{DataStreamWriter, MLSQLForeachBatchRunner}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import streaming.core.datasource.{DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec}
import tech.mlsql.job.{JobManager, MLSQLJobType}

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

        val ssel = context.execListener.clone(sparkSessionForStream)


        val jobInfo = JobManager.getJobInfo(context.owner, MLSQLJobType.SCRIPT, context.groupId, options("code"), -1l)
        jobInfo.copy(jobName = jobInfo.jobName + ":" + jobInfo.groupId)
        
        JobManager.run(sparkSessionForStream, jobInfo, () => {
          val newContext = new MLSQLExecuteContext(ssel, context.owner, context.home, jobInfo.groupId, context.userDefinedParam)
          ScriptSQLExec.setContext(newContext)
          //clear any env from parent
          val skipAuth = newContext.execListener.env().getOrElse("SKIP_AUTH", "false").toBoolean
          newContext.execListener.env().clear()
          ScriptSQLExec.parse(options("code"), newContext.execListener, false, skipAuth, false, false)
        })
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