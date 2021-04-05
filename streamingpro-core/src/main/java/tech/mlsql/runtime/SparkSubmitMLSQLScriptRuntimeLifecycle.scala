package tech.mlsql.runtime

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.{JobManager, RunScriptExecutor}

import scala.io.Source

class SparkSubmitMLSQLScriptRuntimeLifecycle extends MLSQLRuntimeLifecycle with Logging {

  override def beforeRuntimeStarted(params: Map[String, String], conf: SparkConf): Unit = {}

  override def afterRuntimeStarted(params: Map[String, String], conf: SparkConf, rootSparkSession: SparkSession): Unit = {
    val mlsql_path = params("streaming.mlsql.script.path")
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
      for(line <- file.getLines()){
        sql.append(line).append("\n")
      }
    }

    val executor = new RunScriptExecutor(params ++ Map("sql" -> sql.toString, "owner" -> "admin", "jobName" -> "SparkSubmitMLSQLScriptRuntimeLifecycle"))
    JobManager.init(rootSparkSession)
    executor.execute()
    JobManager.shutdown
  }
}
