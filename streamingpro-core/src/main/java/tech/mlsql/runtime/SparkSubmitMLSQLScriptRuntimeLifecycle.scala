package tech.mlsql.runtime

import net.sf.json.JSONArray
import org.apache.commons.lang3.StringUtils
import streaming.core.strategy.platform.{SparkRuntime, StreamingRuntime}
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
class SparkSubmitMLSQLScriptRuntimeLifecycle extends MLSQLPlatformLifecycle with Logging {

  override def afterDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {
    val rootSparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val mlsql_path = params.getOrElse("streaming.mlsql.script.path", "")

    val extraQueryParams = params.filter { case (k, _) => k.startsWith("streaming.mlsql.script.query") }.map { case (k, v) =>
      (k.stripPrefix("streaming.mlsql.script.query."), v)
    }.toMap

    if (StringUtils.isEmpty(mlsql_path)) {
      logWarning(s"The value of parameter 'streaming.mlsql.script.path' is empty.")
      return
    }
    JobManager.init(rootSparkSession)
    try {
      val sql = new StringBuffer()
      if (mlsql_path.startsWith("http") || mlsql_path.startsWith("file")) {
        val file = Source.fromURL(mlsql_path)
        for (line <- file.getLines()) {
          sql.append(line).append("\n")
        }
      } else {
        val array = rootSparkSession.sparkContext.textFile(mlsql_path).collect()
        array.foreach(
          line => {
            sql.append(line).append("\n")
          }
        )
      }

      if (StringUtils.isEmpty(sql.toString)) {
        logWarning(s"The Byzer script file is empty.")
        return
      }

      val executor = new RunScriptExecutor(
        params ++ extraQueryParams ++ Map("sql" -> sql.toString,
          "outputSize" -> "200",
          "fetchType" -> "take",
          "owner" -> params.getOrElse("streaming.mlsql.script.owner", "admin"),
          "async" -> "false",
          "executeMode" -> params.getOrElse("streaming.mlsql.script.executeMode", "query"),
          "jobName" -> params.getOrElse("streaming.mlsql.script.jobName", "SparkSubmitMLSQLScriptRuntimeJob")))
      val (status, res) = executor.execute()
      if (status != 200) {
        throw new RuntimeException(res)
      }
      import rootSparkSession.implicits._

      import scala.collection.JavaConverters._
      val jsonArray = JSONArray.fromObject(res).asScala.map(item => item.toString)
      val df = rootSparkSession.read.json(rootSparkSession.createDataset[String](jsonArray))
      df.show(200, false)

    } catch {
      case e: Exception =>
        logError(s"Run script ${mlsql_path} error: ", e)
    } finally {
      JobManager.shutdown
    }
  }

  override def beforeRuntime(params: Map[String, String]): Unit = {}

  override def afterRuntime(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}

  override def beforeDispatcher(runtime: StreamingRuntime, params: Map[String, String]): Unit = {}
}
