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
import _root_.streaming.dsl.mmlib.algs.tf.cluster.{ClusterSpec, ClusterStatus, ExecutorInfo}
import _root_.streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import _root_.streaming.log.WowLog
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.csdn.annotation.rest.{At, _}
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.mlsql.session.{MLSQLSparkSession, SparkSessionCacheManager}
import org.apache.spark.{MLSQLConf, SparkInstanceService}
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.job.{JobManager, MLSQLJobType}

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
    new Parameter(name = "jobName", required = false, description = "give the job you submit a name. uuid is ok.", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "timeout", required = false, description = "set timeout value for your job. default is -1 which means it is never timeout. millisecond", `type` = "int", allowEmptyValue = false),
    new Parameter(name = "silence", required = false, description = "the last sql in the script will return nothing. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "sessionPerUser", required = false, description = "If set true, the owner will have their own session otherwise share the same. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "sessionPerRequest", required = false, description = "by default false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "async", required = false, description = "If set true ,please also provide a callback url use `callback` parameter and the job will run in background and the API will return.  default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "callback", required = false, description = "Used when async is set true. callback is a url. default: false", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "skipInclude", required = false, description = "disable include statement. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "skipAuth", required = false, description = "disable table authorize . default: true", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "skipGrammarValidate", required = false, description = "validate mlsql grammar. default: true", `type` = "boolean", allowEmptyValue = false)
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

    val htp = findService(classOf[HttpTransportService])
    if (paramAsBoolean("async", false) && !params().containsKey("callback")) {
      render(400, "when async is set true ,then you should set callback url")
    }
    var outputResult: String = "[]"
    try {
      val jobInfo = JobManager.getJobInfo(
        param("owner"), param("jobType", MLSQLJobType.SCRIPT), param("jobName"), param("sql"),
        paramAsLong("timeout", -1L)
      )
      val context = createScriptSQLExecListener(sparkSession, jobInfo.groupId)
      if (paramAsBoolean("async", false)) {
        JobManager.asyncRun(sparkSession, jobInfo, () => {
          try {
            ScriptSQLExec.parse(param("sql"), context,
              skipInclude = paramAsBoolean("skipInclude", false),
              skipAuth = paramAsBoolean("skipAuth", true),
              skipPhysicalJob = paramAsBoolean("skipPhysicalJob", false),
              skipGrammarValidate = paramAsBoolean("skipGrammarValidate", true))

            outputResult = getScriptResult(context, sparkSession)
            htp.post(new Url(param("callback")), Map("stat" -> s"""succeeded""", "res" -> outputResult))
          } catch {
            case e: Exception =>
              e.printStackTrace()
              val msgBuffer = ArrayBuffer[String]()
              if (paramAsBoolean("show_stack", false)) {
                format_full_exception(msgBuffer, e)
              }
              htp.post(new Url(param("callback")), Map("stat" -> s"""failed""", "msg" -> (e.getMessage + "\n" + msgBuffer.mkString("\n"))))
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

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val msgBuffer = ArrayBuffer[String]()
        if (paramAsBoolean("show_stack", false)) {
          format_full_exception(msgBuffer, e)
        }
        render(500, e.getMessage + "\n" + msgBuffer.mkString("\n"))
    } finally {
      cleanActiveSessionInSpark
    }
    render(outputResult)
  }

  private def getScriptResult(context: ScriptSQLExecListener, sparkSession: SparkSession): String = {
    context.getLastSelectTable() match {
      case Some(table) =>
        if (context.env().getOrElse(MLSQLEnvKey.CONTEXT_SYSTEM_TABLE, "false").toBoolean) {
          "[" + WowJsonInferSchema.toJson(sparkSession.table(table)).mkString(",") + "]"
        } else {
          val scriptJsonStringResult = limitOrNot {
            sparkSession.sql(s"select * from $table limit " + paramAsInt("outputSize", 5000))
          }.toJSON.collect().mkString(",")
          "[" + scriptJsonStringResult + "]"
        }
      case None => "[]"
    }
  }


  private def createScriptSQLExecListener(sparkSession: SparkSession, groupId: String) = {

    val allPathPrefix = fromJson(param("allPathPrefix", "{}"), classOf[Map[String, String]])
    val defaultPathPrefix = param("defaultPathPrefix", "")
    val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix)
    val ownerOption = if (params.containsKey("owner")) Some(param("owner")) else None
    var userDefineParams = params.toMap.filter(f => f._1.startsWith("context.")).map(f => (f._1.substring("context.".length), f._2))
    ScriptSQLExec.setContext(new MLSQLExecuteContext(context, param("owner"), context.pathPrefix(None), groupId, userDefineParams))
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


  // tensorflow cluster driver api
  // begin -------------------------------------------
  @At(path = Array("/cluster/register"), types = Array(GET, POST))
  def registerCluster = {
    val Array(host, port) = param("hostAndPort").split(":")
    val jobName = param("jobName")
    val taskIndex = param("taskIndex").toInt
    ClusterSpec.addJob(param("cluster"), ExecutorInfo(host, port.toInt, jobName, taskIndex))
    render(200, "{}")
  }

  // when tensorflow cluster is started ,
  // once the worker finish, it will report to this api
  @At(path = Array("/cluster/worker/status"), types = Array(GET, POST))
  def clusterWorkerStatus = {
    val Array(host, port) = param("hostAndPort").split(":")
    val jobName = param("jobName")
    val taskIndex = param("taskIndex").toInt
    ClusterStatus.count(param("cluster"), ExecutorInfo(host, port.toInt, jobName, taskIndex))
    render(200, "{}")
  }

  // the ps in tensorlfow cluster will check worker status,
  // once all workers finish, then it will kill ps
  @At(path = Array("/cluster/worker/finish"), types = Array(GET, POST))
  def clusterWorkerFinish = {
    val workerFinish = ClusterStatus.count(param("cluster"))
    render(200, workerFinish + "")
  }

  @At(path = Array("/cluster"), types = Array(GET))
  def cluster = {
    render(200, toJsonString(ClusterSpec.clusterSpec(param("cluster"))), ViewType.string)
  }

  //end -------------------------------------------


  @At(path = Array("/check"), types = Array(GET, POST))
  def check = {
    render(WowCollections.map())
  }


  @At(path = Array("/debug/executor/ping"), types = Array(GET, POST))
  def pingExecuotrs = {
    runtime match {
      case sparkRuntime: SparkRuntime =>
        val endpoint = if (sparkRuntime.sparkSession.sparkContext.isLocal) {
          sparkRuntime.localSchedulerBackend.localEndpoint
        } else {
          sparkRuntime.psDriverBackend.psDriverRpcEndpointRef
        }
        endpoint.ask(Message.Ping)
      case _ =>
        throw new RuntimeException(s"unsupport runtime ${runtime.getClass} !")
    }
    render("{}")
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
