package tech.mlsql.runtime

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import streaming.core.strategy.platform.SparkRuntime
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.RunScriptExecutor

class SparkSubmitMLSQLScriptRuntimeLifecycle extends MLSQLRuntimeLifecycle with Logging {

  override def beforeRuntimeStarted(params: Map[String, String], conf: SparkConf): Unit = {}

  override def afterRuntimeStarted(params: Map[String, String], conf: SparkConf, rootSparkSession: SparkSession): Unit = {
    val mlsql_path = params("streaming.mlsql.script.path")
    val array = rootSparkSession.sparkContext.textFile(mlsql_path).collect()
    val sql = new StringBuffer()
    array.foreach(
      line => {
        sql.append(line).append("\n")
      }
    )
    val executor = new RunScriptExecutor(params ++ Map("sql" -> sql.toString, "owner" -> "admin", "jobName" -> "SparkSubmitMLSQLScriptRuntimeLifecycle"))
    executor.execute(rootSparkSession)
  }
}
