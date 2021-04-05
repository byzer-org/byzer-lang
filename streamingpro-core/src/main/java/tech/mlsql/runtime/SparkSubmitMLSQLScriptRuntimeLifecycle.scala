package tech.mlsql.runtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.{JobManager, RunScriptExecutor}

import scala.io.Source

/**
 * When using this plugin, please set these parameters as shown below:
 * streaming.rest=false
 * streaming.spark.service=false
 * streaming.mlsql.script.path=the file path of mlsql script (support for hdfs://, file://, http://)
 * streaming.runtime_hooks=tech.mlsql.runtime.SparkSubmitMLSQLScriptRuntimeLifecycle
 * streaming.mlsql.script.owner=XXXXX
 * streaming.mlsql.script.jobName=SparkSubmitTest
 */
class SparkSubmitMLSQLScriptRuntimeLifecycle extends MLSQLRuntimeLifecycle with Logging {

  override def beforeRuntimeStarted(params: Map[String, String], conf: SparkConf): Unit = {}

  override def afterRuntimeStarted(params: Map[String, String], conf: SparkConf, rootSparkSession: SparkSession): Unit = {
    val mlsql_path = params.getOrElse("streaming.mlsql.script.path", "")
    if (StringUtils.isEmpty(mlsql_path)) {
      logWarning(s"The value of parameter 'streaming.mlsql.script.path' is empty.")
      return
    }
    JobManager.init(rootSparkSession)
    try {
      val sql = new StringBuffer()
      if (!mlsql_path.startsWith("http")) {
        val array = rootSparkSession.sparkContext.textFile(mlsql_path).collect()
        array.foreach(
          line => {
            sql.append(line).append("\n")
          }
        )
      } else {
        val file = Source.fromURL(mlsql_path)
        for (line <- file.getLines()) {
          sql.append(line).append("\n")
        }
      }

      if (StringUtils.isEmpty(sql.toString)) {
        logWarning(s"The mlsql script file is empty.")
        return
      }

      val executor = new RunScriptExecutor(
        params ++ Map("sql" -> sql.toString,
          "owner" -> params.getOrElse("streaming.mlsql.script.owner", "admin"),
          "async" -> "false",
          "executeMode" -> params.getOrElse("streaming.mlsql.script.executeMode", "query"),
          "jobName" -> params.getOrElse("streaming.mlsql.script.jobName", "SparkSubmitMLSQLScriptRuntimeJob")))
      executor.execute()
    } catch {
      case e: Exception =>
        logError(s"Run script ${mlsql_path} error: ", e)
    } finally {
      JobManager.shutdown
    }
  }
}
