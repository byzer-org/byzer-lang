package tech.mlsql.ets

import java.net.URLEncoder

import it.sauronsoftware.cron4j.SchedulingPattern
import org.apache.http.client.fluent.Request
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AsSchedulerService
import tech.mlsql.scheduler.{DependencyJob, TimerJob}
import tech.mlsql.store.DBStore

/**
 * 2019-09-05 WilliamZhu(allwefantasy@gmail.com)
 */
class SchedulerCommand(override val uid: String) extends SQLAlg with WowParams {

  import tech.mlsql.scheduler.client.SchedulerUtils._

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import AsSchedulerService._
    import spark.implicits._

    require(spark.conf.getOption(PREFIX + AsSchedulerServiceKEY).isDefined, s"!scheduler is only can used in MLSQL Engine configured with ${AsSchedulerServiceKEY}")

    val commands = JSONTool.parseJson[List[String]](params("parameters")).toArray
    val context = ScriptSQLExec.context()
    val authSecret = spark.conf.get(PREFIX + CONSOLE_TOKEN)
    val consoleUrl = spark.conf.get(PREFIX + CONSOLE_URL)

    def validateExpr(cronExpr: String): Unit = {
      if (!cronExpr.isEmpty) {
        new SchedulingPattern(cronExpr)
      }
    }

    def getScriptId(path: String) = {
      def encode(str: String) = {
        URLEncoder.encode(str, "utf-8")
      }

      val script = Request.Get(consoleUrl.stripSuffix("/") + s"/api_v1/script_file/path/id?path=${encode(path)}&owner=${encode(context.owner)}")
        .connectTimeout(60 * 1000)
        .socketTimeout(10 * 60 * 1000).addHeader("access-token", authSecret)
        .execute().returnContent().asString()
      script.toInt
    }

    //    val envSession = new SetSession(spark, context.owner)
    //
    //    val schedulerServerTags = envSession.fetchSetStatement match {
    //      case Some(df) => df.collect().filter(f => f.k == "__scheduler__").headOption match {
    //        case Some(oww) => oww.v
    //        case None => null
    //      }
    //      case None => null
    //    }
    //    require(schedulerServerTags != null,
    //      """
    //        |Try to set scheduler server tags, for example:
    //        |
    //        |!scheduler conf "jack___scheduler__";
    //      """.stripMargin)

    commands match {
      case Array(id, "with", cronExpr) =>
        validateExpr(cronExpr)
        val scriptId = getScriptId(id)
        val df = spark.createDataset(Seq(TimerJob(context.owner, scriptId, cronExpr))).toDF()
        DBStore.store.saveTable(spark, df, SCHEDULER_TIME_JOBS, Option("id"), cronExpr.isEmpty)
        if (cronExpr.isEmpty) {
          val removeDf = DBStore.store.readTable(spark, SCHEDULER_DEPENDENCY_JOBS).where(F.col("dependency") === scriptId)
          DBStore.store.saveTable(spark, removeDf, SCHEDULER_DEPENDENCY_JOBS, Option("id,dependency"), true)
        }
        DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS)

      case Array(id, "depends", "on", dependedIds) =>
        val timeJobs = DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS).withColumn("id", F.col("id").cast(IntegerType)).as[TimerJob[Int]].collect().map(f => f.id).toSet
        val jobs = dependedIds.split(",").map { f =>
          val depId = getScriptId(f)
          require(timeJobs.contains(depId), s"${depId} should be timer job")
          DependencyJob(context.owner, getScriptId(id), depId)
        }
        val df = spark.createDataset(jobs).toDF()
        DBStore.store.saveTable(spark, df, SCHEDULER_DEPENDENCY_JOBS, Option("id,dependency"), false)
        DBStore.store.tryReadTable(spark, SCHEDULER_DEPENDENCY_JOBS, () => spark.createDataset[DependencyJob[Int]](Seq()).toDF())

      case Array("remove", id, "depends", "on", dependedIds) =>
        val timeJobs = DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS).withColumn("id", F.col("id").cast(IntegerType)).as[TimerJob[Int]].collect().map(f => f.id).toSet
        val jobs = dependedIds.split(",").map { f =>
          val depId = getScriptId(f)
          require(timeJobs.contains(depId), s"${depId} should be timer job")
          DependencyJob(context.owner, getScriptId(id), depId)
        }
        val df = spark.createDataset(jobs).toDF()
        DBStore.store.saveTable(spark, df, SCHEDULER_DEPENDENCY_JOBS, Option("id,dependency"), true)
        DBStore.store.tryReadTable(spark, SCHEDULER_DEPENDENCY_JOBS, () => spark.createDataset[DependencyJob[Int]](Seq()).toDF())

      case Array("remove", name) =>
        val id = getScriptId(name)
        val timeJob = DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS).withColumn("id", F.col("id").cast(IntegerType)).as[TimerJob[Int]].filter(f => f.id == id)
        DBStore.store.saveTable(spark, timeJob.toDF(), SCHEDULER_TIME_JOBS, Option("id"), true)
        DBStore.store.tryReadTable(spark, SCHEDULER_TIME_JOBS, () => spark.createDataset[TimerJob[Int]](Seq()).toDF())

      case Array("remove", "id", id) =>
        val timeJob = DBStore.store.readTable(spark, SCHEDULER_TIME_JOBS).withColumn("id", F.col("id").cast(IntegerType)).as[TimerJob[Int]].filter(f => f.id == id.toInt)
        DBStore.store.saveTable(spark, timeJob.toDF(), SCHEDULER_TIME_JOBS, Option("id"), true)
        DBStore.store.tryReadTable(spark, SCHEDULER_TIME_JOBS, () => spark.createDataset[TimerJob[Int]](Seq()).toDF())

      case Array("list") =>
        DBStore.store.tryReadTable(spark, SCHEDULER_TIME_JOBS, () => spark.createDataset[TimerJob[Int]](Seq()).toDF())
      case Array("list", "depends") =>
        DBStore.store.tryReadTable(spark, SCHEDULER_DEPENDENCY_JOBS, () => spark.createDataset[DependencyJob[Int]](Seq()).toDF())

    }
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException(s"${getClass.getName} not support register ")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException(s"${getClass.getName} not support register ")

}



