package tech.mlsql.job

import net.csdn.ServiceFramwork
import net.csdn.common.path.Url
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.MLSQLConf
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.mlsql.session.MLSQLSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import streaming.log.WowLog
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.app.{CustomController, ResultResp}
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.runtime.plugins.exception_render.ExceptionRenderManager
import tech.mlsql.runtime.plugins.request_cleaner.RequestCleanerManager
import tech.mlsql.runtime.plugins.result_render.ResultRenderManager

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 9/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class RunScriptExecutor(_params: Map[String, String]) extends Logging with WowLog {
  private val extraParams = mutable.HashMap[String, String]()
  private var _autoClean = false

  def sql(sql: String) = {
    extraParams += ("sql" -> sql)
    this
  }

  def owner(owner: String) = {
    extraParams += ("owner" -> owner)
    this
  }

  def async(async: Boolean) = {
    extraParams += ("async" -> async.toString)
    this
  }

  def timeout(timeout: Long) = {
    extraParams += ("timeout" -> timeout.toString)
    this
  }

  def executeMode(executeMode: String) = {
    extraParams += ("executeMode" -> executeMode)
    this
  }

  def autoClean(autoClean: Boolean) = {
    this._autoClean = autoClean
    this
  }

  def simpleExecute(): Option[DataFrame] = {
    val sparkSession = getSession
    try {
      val jobInfo = JobManager.getJobInfo(
        param("owner"), param("jobType", MLSQLJobType.SCRIPT), param("jobName"), param("sql"),
        paramAsLong("timeout", -1L)
      )
      val context = createScriptSQLExecListener(sparkSession, jobInfo.groupId)

      JobManager.run(sparkSession, jobInfo, () => {
        ScriptSQLExec.parse(param("sql"), context,
          skipInclude = paramAsBoolean("skipInclude", false),
          skipAuth = paramAsBoolean("skipAuth", true),
          skipPhysicalJob = paramAsBoolean("skipPhysicalJob", false),
          skipGrammarValidate = paramAsBoolean("skipGrammarValidate", true)
        )
      })
      context.getLastSelectTable() match {
        case Some(tableName) =>
          Option(sparkSession.table(tableName))
        case None =>
          None
      }
    } finally {
      if(this._autoClean){
        RequestCleanerManager.call()
        cleanActiveSessionInSpark
      }
    }
  }

  def execute(): (Int, String) = {
    val silence = paramAsBoolean("silence", false)
    val sparkSession = getSession
    val htp = findService[HttpTransportService](classOf[HttpTransportService])
    val includeSchema = param("includeSchema", "false").toBoolean
    var outputResult: String = if (includeSchema) "{}" else "[]"
    try {
      val jobInfo = JobManager.getJobInfo(
        param("owner"), param("jobType", MLSQLJobType.SCRIPT), param("jobName"), param("sql"),
        paramAsLong("timeout", -1L)
      )
      val context = createScriptSQLExecListener(sparkSession, jobInfo.groupId)

      def query = {
        if (paramAsBoolean("async", false)) {
          JobManager.asyncRun(sparkSession, jobInfo, () => {
            try {
              ScriptSQLExec.parse(param("sql"), context,
                skipInclude = paramAsBoolean("skipInclude", false),
                skipAuth = paramAsBoolean("skipAuth", true),
                skipPhysicalJob = paramAsBoolean("skipPhysicalJob", false),
                skipGrammarValidate = paramAsBoolean("skipGrammarValidate", true))

              outputResult = getScriptResult(context, sparkSession)
              htp.post(new Url(param("callback")),
                Map("stat" -> s"""succeeded""",
                  "res" -> outputResult,
                  "jobInfo" -> JSONTool.toJsonStr(jobInfo)).asJava)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                val msgBuffer = ArrayBuffer[String]()
                if (paramAsBoolean("show_stack", false)) {
                  format_full_exception(msgBuffer, e)
                }
                htp.post(new Url(param("callback")),
                  Map("stat" -> s"""failed""",
                    "msg" -> (e.getMessage + "\n" + msgBuffer.mkString("\n")),
                    "jobInfo" -> JSONTool.toJsonStr(jobInfo)
                  ).asJava)
            }
          })
        } else {
          JobManager.run(sparkSession, jobInfo, () => {
            ScriptSQLExec.parse(param("sql"), context,
              skipInclude = paramAsBoolean("skipInclude", false),
              skipAuth = paramAsBoolean("skipAuth", true),
              skipPhysicalJob = paramAsBoolean("skipPhysicalJob", false),
              skipGrammarValidate = paramAsBoolean("skipGrammarValidate", true)
            )
            if (!silence) {
              outputResult = getScriptResult(context, sparkSession)
            }
          })
        }
      }

      def analyze = {
        ScriptSQLExec.parse(param("sql"), context,
          skipInclude = false,
          skipAuth = true,
          skipPhysicalJob = true,
          skipGrammarValidate = true)
        context.preProcessListener.map(f => JSONTool.toJsonStr(f.analyzedStatements.map(_.unwrap))) match {
          case Some(i) => outputResult = i
          case None =>
        }
      }

      params.getOrElse("executeMode", "query") match {
        case "query" => query
        case "analyze" => analyze
        case executeMode: String =>
          AppRuntimeStore.store.getController(executeMode) match {
            case Some(item) =>
              outputResult = Class.forName(item.customClassItem.className).
                newInstance().asInstanceOf[CustomController].run(params().toMap + ("__jobinfo__" -> JSONTool.toJsonStr(jobInfo)))
            case None => throw new RuntimeException(s"no executeMode named ${executeMode}")
          }
      }

    } catch {
      case e: Exception =>
        val msg = ExceptionRenderManager.call(e)
        return (500, msg.str.get)
    } finally {
      if(this._autoClean){
        RequestCleanerManager.call()
        cleanActiveSessionInSpark
      }
    }
    return (200, outputResult)
  }

  /**
   * | enable limit | global | maxResultSize | condition                       | result           |
   * | ------------ | ------ | ------------- | ------------------------------- | ---------------- |
   * | true         | -1     | -1            | N/A                             | defualt = 1000   |
   * | true         | -1     | Int           | N/A                             | ${maxResultSize} |
   * | true         | Int    | -1            | Or ${maxResultSize} > ${global} | ${global}        |
   * | true         | Int    | Int           | AND ${maxResultSize} < ${global}| ${maxResultSize} |
   *
   * when we enable result size limitation, the size of result should <= ${maxSize} <= ${global}
   *
   * @param ds
   * @param maxSize
   * @tparam T
   * @return
   */
  private def limitOrNot[T](ds: Dataset[T], maxSize: Int = paramAsInt("maxResultSize", -1)): Dataset[T] = {
    var result = ds
    val globalLimit = ds.sparkSession.sparkContext.getConf.getInt(
      MLSQLConf.RESTFUL_API_MAX_RESULT_SIZE.key, -1
    )
    if (ds.sparkSession.sparkContext.getConf.getBoolean(MLSQLConf.ENABLE_MAX_RESULT_SIZE.key, false)) {
      if (globalLimit == -1) {
        if (maxSize == -1) {
          result = ds.limit(1000)
        } else {
          result = ds.limit(maxSize)
        }
      } else {
        if (maxSize == -1 || maxSize > globalLimit) {
          result = ds.limit(globalLimit)
        } else {
          result = ds.limit(maxSize)
        }
      }
    }
    result
  }

  private def getScriptResult(context: ScriptSQLExecListener, sparkSession: SparkSession): String = {
    val result = new StringBuffer()
    val includeSchema = param("includeSchema", "false").toBoolean
    val fetchType = param("fetchType", "collect")
    if (includeSchema) {
      result.append("{")
    }
    context.getLastSelectTable() match {
      case Some(table) =>
        // result hook
        var df = sparkSession.table(table)
        df = ResultRenderManager.call(ResultResp(df, table)).df
        if (includeSchema) {
          result.append(s""" "schema":${df.schema.json},"data": """)
        }

        if (context.env().getOrElse(MLSQLEnvKey.CONTEXT_SYSTEM_TABLE, "false").toBoolean) {
          result.append("[" + WowJsonInferSchema.toJson(df).mkString(",") + "]")
        } else {
          val outputSize = paramAsInt("outputSize", 5000)
          val jsonDF = limitOrNot {
            sparkSession.sql(s"select * from $table limit " + outputSize)
          }.toJSON
          val scriptJsonStringResult = fetchType match {
            case "collect" => jsonDF.collect().mkString(",")
            case "take" => sparkSession.table(table).toJSON.take(outputSize).mkString(",")
          }
          result.append("[" + scriptJsonStringResult + "]")
        }
      case None => result.append("[]")
    }
    if (includeSchema) {
      result.append("}")
    }
    return result.toString
  }

  private def createScriptSQLExecListener(sparkSession: SparkSession, groupId: String) = {

    val allPathPrefix = JSONTool.parseJson[Map[String, String]](param("allPathPrefix", "{}"))
    val defaultPathPrefix = param("defaultPathPrefix", "")
    val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix)
    val ownerOption = if (params.contains("owner")) Some(param("owner")) else None
    val userDefineParams = params.filter(f => f._1.startsWith("context.")).map(f => (f._1.substring("context.".length), f._2))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, param("owner"), context.pathPrefix(None), groupId,
      userDefineParams ++ Map("__PARAMS__" -> JSONTool.toJsonStr(params()))
    ))
    context.addEnv("SKIP_AUTH", param("skipAuth", "true"))
    context.addEnv("HOME", context.pathPrefix(None))
    context.addEnv("OWNER", ownerOption.getOrElse("anonymous"))
    context
  }

  private def param(str: String) = {
    params.getOrElse(str, null)
  }

  private def param(str: String, defaultV: String) = {
    params.getOrElse(str, defaultV)
  }

  private def paramAsBoolean(str: String, defaultV: Boolean) = {
    params.getOrElse(str, defaultV.toString).toBoolean
  }

  private def paramAsLong(str: String, defaultV: Long) = {
    params.getOrElse(str, defaultV.toString).toLong
  }

  private def paramAsInt(str: String, defaultV: Int) = {
    params.getOrElse(str, defaultV.toString).toInt
  }

  private def hasParam(str: String) = {
    params.contains(str)
  }

  private def params() = {
    _params ++ extraParams
  }

  def runtime = PlatformManager.getRuntime

  def getSession = {

    val session = if (paramAsBoolean("sessionPerUser", false)) {
      runtime.asInstanceOf[SparkRuntime].getSession(param("owner", "admin"))
    } else {
      runtime.asInstanceOf[SparkRuntime].sparkSession
    }

    if (paramAsBoolean("sessionPerRequest", false)) {
      MLSQLSparkSession.cloneSession(session)
    } else {
      session
    }
  }

  def getSessionByOwner(owner: String) = {
    if (paramAsBoolean("sessionPerUser", false)) {
      runtime.asInstanceOf[SparkRuntime].getSession(owner)
    } else {
      runtime.asInstanceOf[SparkRuntime].sparkSession
    }
  }

  def cleanActiveSessionInSpark = {
    ScriptSQLExec.unset
    SparkSession.clearActiveSession()
  }

  private def findService[T](clzz: Class[T]): T = ServiceFramwork.injector.getInstance(clzz)

}
