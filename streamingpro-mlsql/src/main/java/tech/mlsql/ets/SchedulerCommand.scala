package tech.mlsql.ets

import java.net.URLEncoder

import it.sauronsoftware.cron4j.SchedulingPattern
import org.apache.http.client.fluent.Request
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.Functions
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.datalake.DataLake
import tech.mlsql.scheduler.algorithm.TimeScheduler
import tech.mlsql.scheduler.client.SchedulerTaskStore
import tech.mlsql.scheduler.{DependencyJob, TimerJob}

/**
  * 2019-09-05 WilliamZhu(allwefantasy@gmail.com)
  */
class SchedulerCommand(override val uid: String) extends SQLAlg with Functions with WowParams {

  import SchedulerCommand._

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val timezoneID = spark.sessionState.conf.sessionLocalTimeZone
    val commands = JSONTool.parseJson[List[String]](params("parameters")).toArray
    val context = ScriptSQLExec.context()
    val authSecret = context.userDefinedParam.filter(f => f._1 == "__auth_secret__").head._2
    val consoleUrl = context.userDefinedParam.filter(f => f._1 == "__default__console_url__").head._2

    def validateExpr(cronExpr: String) = new SchedulingPattern(cronExpr)

    def getScriptId(path: String) = {
      def encode(str: String) = {
        URLEncoder.encode(str, "utf-8")
      }

      val script = Request.Get(PathFun(consoleUrl).add(s"/api_v1/script_file/path/id?path=${encode(path)}&owner=${encode(context.owner)}").toPath)
        .connectTimeout(60 * 1000)
        .socketTimeout(10 * 60 * 1000).addHeader("access-token", authSecret)
        .execute().returnContent().asString()
      script.toInt
    }


    TimeScheduler.start(new SchedulerTaskStore(spark, consoleUrl, authSecret), timezoneID)
    commands match {
      case Array(id, "with", cronExpr) =>
        validateExpr(cronExpr)
        val df = spark.createDataset(Seq(TimerJob(context.owner, getScriptId(id), cronExpr))).toDF()
        saveTable(spark, df, SCHEDULER_TIME_JOBS)
        readTable(spark, SCHEDULER_TIME_JOBS)
      case Array(id, "depends", "on", dependedIds) =>
        val timeJobs = readTable(spark, SCHEDULER_TIME_JOBS).as[TimerJob[Int]].collect().map(f => f.id).toSet
        val jobs = dependedIds.split(",").map { f =>
          val depId = getScriptId(f)
          require(timeJobs.contains(depId), s"${depId} should be timer job")
          DependencyJob(context.owner, getScriptId(id), depId)
        }
        val df = spark.createDataset(jobs).toDF()
        saveTable(spark, df, SCHEDULER_DEPENDENCY_JOBS)
        readTable(spark, SCHEDULER_DEPENDENCY_JOBS)

    }
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException(s"${getClass.getName} not support register ")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException(s"${getClass.getName} not support register ")

}

object SchedulerCommand {
  val DELTA_FORMAT = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"
  val SCHEDULER_DEPENDENCY_JOBS = "scheduler.dependency_jobs"
  val SCHEDULER_TIME_JOBS = "scheduler.time_jobs"
  val SCHEDULER_TIME_JOBS_STATUS = "scheduler.time_jobs_status"

  def saveTable(spark: SparkSession, data: DataFrame, tableName: String) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    data.write.format(DELTA_FORMAT).option("idCols", "id").
      mode(SaveMode.Append).save(finalPath)
  }

  def readTable(spark: SparkSession, tableName: String) = {
    val dataLake = new DataLake(spark)
    require(dataLake.isEnable, "please set -streaming.datalake.path enable delta db mode")
    val finalPath = dataLake.identifyToPath(tableName)
    spark.read.format(DELTA_FORMAT).load(finalPath)
  }

}

