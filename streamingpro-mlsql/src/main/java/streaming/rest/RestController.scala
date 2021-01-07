/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.rest

import _root_.streaming.core._
import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import _root_.streaming.log.WowLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.csdn.annotation.rest.{At, _}
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.ps.cluster.Message
import org.apache.spark.ps.cluster.Message.Pong
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.mlsql.session.{MLSQLSparkSession, SparkSessionCacheManager}
import org.apache.spark.{MLSQLConf, SparkInstanceService}
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.app.{CustomController, ResultResp}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.job.{JobManager, MLSQLJobType}
import tech.mlsql.runtime.AppRuntimeStore
import tech.mlsql.runtime.plugins.exception_render.ExceptionRenderManager
import tech.mlsql.runtime.plugins.request_cleaner.RequestCleanerManager
import tech.mlsql.runtime.plugins.result_render.ResultRenderManager

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Created by allwefantasy on 28/3/2017.
 */
@OpenAPIDefinition(
  info = new BasicInfo(
    desc = "The collection of rest api are used to execute SQL, manager jobs and download hdfs file.",
    state = State.alpha,
    contact = new Contact(url = "https://github.com/allwefantasy", name = "WilliamZhu", email = "allwefantasy@gmail.com"),
    license = new License(name = "Apache-2.0", url = "https://github.com/allwefantasy/streamingpro/blob/master/LICENSE")),
  externalDocs = new ExternalDocumentation(description =
    """

    """),
  servers = Array()
)
class RestController extends ApplicationController with WowLog {

  // mlsql script execute api, support async and sysn
  // begin -------------------------------------------

  @Action(
    summary = "used to execute MLSQL script", description = "async/sync supports"
  )
  @Parameters(Array(
    new Parameter(name = "sql", required = true, description = "MLSQL script content", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "owner", required = false,
      description = "the user who execute this API and also will be used in MLSQL script automatically. " +
        "default: admin. Please set this owner properly.",
      `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "jobType", required = false, description = "script|stream|sql; default is script", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "executeMode", required = false, description = "query|analyze; default is query", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "jobName", required = false, description = "give the job you submit a name. uuid is ok.", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "timeout", required = false, description = "set timeout value for your job. default is -1 which means it is never timeout. millisecond", `type` = "int", allowEmptyValue = false),
    new Parameter(name = "silence", required = false, description = "the last sql in the script will return nothing. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "sessionPerUser", required = false, description = "If set true, the owner will have their own session otherwise share the same. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "sessionPerRequest", required = false, description = "by default false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "async", required = false, description = "If set true ,please also provide a callback url use `callback` parameter and the job will run in background and the API will return.  default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "callback", required = false, description = "Used when async is set true. callback is a url. default: false", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "skipInclude", required = false, description = "disable include statement. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "skipAuth", required = false, description = "disable table authorize . default: true", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "skipGrammarValidate", required = false, description = "validate mlsql grammar. default: true", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "includeSchema", required = false, description = "the return value should contains schema info. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "fetchType", required = false, description = "take/collect. default: collect", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "enableQueryWithIndexer", required = false, description = "try query with indexer to speed. default: false", `type` = "boolean", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/run/script"), types = Array(GET, POST))
  def script = {
    setAccessControlAllowOrigin

    val silence = paramAsBoolean("silence", false)
    val sparkSession = getSession

    accessAuth(sparkSession)

    val htp = findService(classOf[HttpTransportService])
    if (paramAsBoolean("async", false) && !params().containsKey("callback")) {
      render(400, "when async is set true ,then you should set callback url")
    }
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
                  "jobInfo" -> JSONTool.toJsonStr(jobInfo)))
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
                  ))
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

      params.getOrDefault("executeMode", "query") match {
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
        render(500, msg.str.get)
    } finally {
      RequestCleanerManager.call()
      cleanActiveSessionInSpark
    }
    render(outputResult)
  }

  private def accessAuth(sparkSession: SparkSession) = {
    val accessToken = sparkSession.conf.get("spark.mlsql.auth.access_token", "")
    if (!accessToken.isEmpty) {
      if (param("access_token") != accessToken) {
        render(403, JSONTool.toJsonStr(Map("msg" -> "access_token is not right")))
      }
    }

    val customAuth = sparkSession.conf.get("spark.mlsql.auth.custom", "")
    if (!customAuth.isEmpty) {
      import scala.collection.JavaConverters._
      val restParams = params().asScala.toMap
      val (isOk, message) = Class.forName(customAuth).newInstance().asInstanceOf[ {def auth(params: Map[String, String]): (Boolean, String)}].auth(restParams)
      if (!isOk) {
        render(403, JSONTool.toJsonStr(Map("msg" -> message)))
      }
    }
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

    val allPathPrefix = fromJson(param("allPathPrefix", "{}"), classOf[Map[String, String]])
    val defaultPathPrefix = param("defaultPathPrefix", "")
    val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix)
    val ownerOption = if (params.containsKey("owner")) Some(param("owner")) else None
    val userDefineParams = params.toMap.filter(f => f._1.startsWith("context.")).map(f => (f._1.substring("context.".length), f._2))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, param("owner"), context.pathPrefix(None), groupId,
      userDefineParams ++ Map("__PARAMS__" -> JSONTool.toJsonStr(params()))
    ))
    context.addEnv("SKIP_AUTH", param("skipAuth", "true"))
    context.addEnv("HOME", context.pathPrefix(None))
    context.addEnv("OWNER", ownerOption.getOrElse("anonymous"))
    context
  }

  // download hdfs file
  @Action(
    summary = "download file from hdfs", description = "tar/raw supports"
  )
  @Parameters(Array(
    new Parameter(name = "paths", required = false, description = "the paths you want download", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "fileType", required = false, description = "raw will combine all files to one file; tar will package all files; tar|raw default: raw", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "fileName", required = false, description = "the filename you want after downloaded", `type` = "string", allowEmptyValue = false)

  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "text",
      schema = new Schema(`type` = "string", format = """success""", description = "")
    ))
  ))
  @At(path = Array("/download"), types = Array(GET, POST))
  def download = {
    intercept()
    accessAuth(getSession)
    val filename = param("fileName", System.currentTimeMillis() + "")
    param("fileType", "raw") match {
      case "tar" =>
        restResponse.httpServletResponse().setContentType("application/octet-stream")
        restResponse.httpServletResponse().addHeader("Content-Disposition", "attachment;filename=" + new String((filename + ".tar").getBytes))
        restResponse.httpServletResponse().addHeader("Transfer-Encoding", "chunked")
        DownloadRunner.getTarFileByPath(restResponse.httpServletResponse(), param("paths")) match {
          case 200 => render("success")
          case 400 => render(400, "download fail")
          case 500 => render(500, "server error")
        }
      case "raw" =>
        restResponse.httpServletResponse().setContentType("application/octet-stream")
        restResponse.httpServletResponse().addHeader("Content-Disposition", "attachment;filename=" + new String((filename + "." + param("file_suffix")).getBytes))
        restResponse.httpServletResponse().addHeader("Transfer-Encoding", "chunked")
        DownloadRunner.getRawFileByPath(restResponse.httpServletResponse(), param("paths"), paramAsLong("pos", 0)) match {
          case 200 => render("success")
          case 400 => render(400, "download fail")
          case 500 => render(500, "server error")
        }

    }

  }

  //end -------------------------------------------

  // job manager api
  // begin --------------------------------------------------------
  @Action(
    summary = "show all running jobs", description = ""
  )
  @Parameters(Array())
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/runningjobs"), types = Array(GET, POST))
  def getRunningJobGroup = {
    setAccessControlAllowOrigin
    accessAuth(getSession)
    val infoMap = JobManager.getJobInfo
    render(200, toJsonString(infoMap))
  }

  def setAccessControlAllowOrigin = {
    try {
      restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
    } catch {
      case e: RuntimeException =>
    }
  }


  /*
      Notice that JobManager also needs to get spark session. When we change here, please
      be careful and do not break the JobManager.
  */
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

  @Action(
    summary = "kill specific job", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "groupId", required = false, description = "the job id", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "jobName", required = false, description = "the job name", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/killjob"), types = Array(GET, POST))
  def killJob = {
    setAccessControlAllowOrigin
    accessAuth(getSession)
    val groupId = param("groupId")
    if (groupId == null) {
      val jobName = param("jobName")
      val groupIds = JobManager.getJobInfo.filter(f => f._2.jobName == jobName)
      groupIds.headOption match {
        case Some(item) => JobManager.killJob(getSessionByOwner(item._2.owner), item._2.groupId)
        case None =>
      }
    } else {
      JobManager.getJobInfo.filter(f => f._2.groupId == groupId).headOption match {
        case Some(item) => JobManager.killJob(getSessionByOwner(item._2.owner), item._2.groupId)
        case None =>
      }
    }
    cleanActiveSessionInSpark
    render(200, "{}")
  }

  @Action(
    summary = "logout user", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "owner", required = true, description = "the user you want to logout", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "sessionPerUser", required = true, description = "make sure sessionPerUser is set true", `type` = "boolean", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/user/logout"), types = Array(GET, POST))
  def userLogout = {
    setAccessControlAllowOrigin
    accessAuth(getSession)
    require(hasParam("owner"), "owner is should be set ")
    if (paramAsBoolean("sessionPerUser", false)) {
      val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
      SparkSessionCacheManager.get.closeSession(param("owner", ""))
      cleanActiveSessionInSpark
      render(200, toJsonString(Map("msg" -> "success")))
    } else {
      render(400, toJsonString(Map("msg" -> "please make sure sessionPerUser is set to true")))
    }

  }

  // end --------------------------------------------------------


  @At(path = Array("/check"), types = Array(GET, POST))
  def check = {
    render(WowCollections.map())
  }


  @At(path = Array("/debug/executor/ping"), types = Array(GET, POST))
  def pingExecuotrs = {
    val pongs = runtime match {
      case sparkRuntime: SparkRuntime =>
        val endpoint = sparkRuntime.psDriverBackend.psDriverRpcEndpointRef
        val pongs = endpoint.askSync[List[Pong]](Message.Ping)
        pongs
      case _ =>
        throw new RuntimeException(s"unsupport runtime ${runtime.getClass} !")
    }
    render(JSONTool.toJsonStr(pongs))
  }


  @At(path = Array("/instance/resource"), types = Array(GET, POST))
  def instanceResource = {
    val session = getSession
    val resource = new SparkInstanceService(session).resources
    cleanActiveSessionInSpark
    render(toJsonString(resource))
  }

  //end -------------------------------------------

  // help method
  // begin --------------------------------------------------------
  def runtime = PlatformManager.getRuntime

  private[this] val _mapper = new ObjectMapper()
  _mapper.registerModule(DefaultScalaModule)

  def toJsonString[T](obj: T): String = {
    _mapper.writeValueAsString(obj)
  }

  def fromJson[T](json: String, `class`: Class[T]): T = {
    try {
      _mapper.readValue(json, `class`)
    } catch {
      case NonFatal(e) =>
        logger.error(s"parse json error.", e)
        null.asInstanceOf[T]
    }
  }

  def _save(dfWriter: DataFrameWriter[Row]) = {
    if (hasParam("tableName")) {
      dfWriter.saveAsTable(param("tableName"))
    } else {
      dfWriter.format(param("format", "csv")).save(param("path", "-"))
    }
  }

  def intercept() = {
    val jparams = runtime.asInstanceOf[SparkRuntime].params
    if (jparams.containsKey("streaming.rest.intercept.clzz")) {
      val interceptor = Class.forName(jparams("streaming.rest.intercept.clzz").toString).newInstance()
      interceptor.asInstanceOf[RestInterceptor].before(request = request.httpServletRequest(), response = restResponse.httpServletResponse())
    }
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

  // end --------------------------------------------------------
}
