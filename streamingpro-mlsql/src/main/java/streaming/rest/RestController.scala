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

import java.lang.reflect.Modifier

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.csdn.annotation.rest.{At, _}
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.{MLSQLConf, SparkInstanceService}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql._
import org.joda.time.format.ISODateTimeFormat
import _root_.streaming.common.JarUtil
import _root_.streaming.core._
import _root_.streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import _root_.streaming.dsl.mmlib.algs.tf.cluster.{ClusterSpec, ClusterStatus, ExecutorInfo}
import _root_.streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import scala.collection.JavaConversions._
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
class RestController extends ApplicationController {

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
    new Parameter(name = "async", required = false, description = "If set true ,please also provide a callback url use `callback` parameter and the job will run in background and the API will return.  default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "callback", required = false, description = "Used when async is set true. callback is a url. default: false", `type` = "string", allowEmptyValue = false),
    new Parameter(name = "skipInclude", required = false, description = "disable include statement. default: false", `type` = "boolean", allowEmptyValue = false),
    new Parameter(name = "skipAuth", required = false, description = "disable table authorize . default: true", `type` = "boolean", allowEmptyValue = false)
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
    val sparkSession = if (paramAsBoolean("sessionPerUser", false)) {
      runtime.asInstanceOf[SparkRuntime].getSession(param("owner", "admin"))
    } else {
      runtime.asInstanceOf[SparkRuntime].sparkSession
    }

    val htp = findService(classOf[HttpTransportService])
    if (paramAsBoolean("async", false) && !params().containsKey("callback")) {
      render(400, "when async is set true ,then you should set callback url")
    }
    var outputResult: String = "[]"
    try {
      val jobInfo = StreamingproJobManager.getStreamingproJobInfo(
        param("owner"), param("jobType", StreamingproJobType.SCRIPT), param("jobName"), param("sql"),
        paramAsLong("timeout", -1L)
      )
      if (paramAsBoolean("async", false)) {
        StreamingproJobManager.asyncRun(sparkSession, jobInfo, () => {
          try {
            val context = createScriptSQLExecListener(sparkSession, jobInfo.groupId)
            ScriptSQLExec.parse(param("sql"), context, paramAsBoolean("skipInclude", false), paramAsBoolean("skipAuth", true))
            htp.get(new Url(param("callback")), Map("stat" -> s"""success"""))
          } catch {
            case e: Exception =>
              e.printStackTrace()
              htp.get(new Url(param("callback")), Map("fail" -> s"""success"""))
          }
        })
      } else {
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          val context = createScriptSQLExecListener(sparkSession, jobInfo.groupId)
          ScriptSQLExec.parse(param("sql"), context, paramAsBoolean("skipInclude", false), paramAsBoolean("skipAuth", true))
          if (!silence) {
            outputResult = context.getLastSelectTable() match {
              case Some(table) =>
                val scriptJsonStringResult = limitOrNot {
                  sparkSession.sql(s"select * from $table limit " + paramAsInt("outputSize", 5000))
                }.toJSON.collect().mkString(",")
                "[" + scriptJsonStringResult + "]"
              case None => "[]"
            }
          }
        })
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val msg = if (paramAsBoolean("show_stack", false)) e.getStackTrace.map(f => f.toString).mkString("\n") else ""
        render(500, e.getMessage + "\n" + msg)
    }
    render(outputResult)
  }

  private def createScriptSQLExecListener(sparkSession: SparkSession, groupId: String) = {

    val allPathPrefix = fromJson(param("allPathPrefix", "{}"), classOf[Map[String, String]])
    val defaultPathPrefix = param("defaultPathPrefix", "")
    val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix)
    val ownerOption = if (params.containsKey("owner")) Some(param("owner")) else None
    val userDefineParams = params.toMap.filter(f => f._1.startsWith("context.")).map(f => (f._1.substring("context.".length), f._2)).toMap
    ScriptSQLExec.setContext(new MLSQLExecuteContext(param("owner"), context.pathPrefix(None), groupId, userDefineParams))
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


  // single spark sql support
  // begin -------------------------------------------
  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def ddlSql = {
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sparkSession = sparkRuntime.sparkSession
    val path = param("path", "-")
    if (paramAsBoolean("async", false) && path == "-") {
      render(s"""path should not be empty""")
    }

    val jobInfo = StreamingproJobManager.getStreamingproJobInfo(
      param("owner"), StreamingproJobType.SQL, param("jobName"), param("sql"),
      paramAsLong("timeout", 30000)
    )

    paramAsBoolean("async", false).toString match {
      case "true" if hasParam("callback") =>
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          val dfWriter = limitOrNot(
            sparkSession.sql(param("sql"))
          ).write
            .mode(SaveMode.Overwrite)
          _save(dfWriter)
          val htp = findService(classOf[HttpTransportService])
          import scala.collection.JavaConversions._
          htp.get(new Url(param("callback")), Map("url_path" -> s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path"""))
        })
        render("[]")

      case "false" if param("resultType", "") == "file" =>
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          val dfWriter = limitOrNot(
            sparkSession.sql(param("sql"))
          ).write
            .mode(SaveMode.Overwrite)
          _save(dfWriter)
          render(s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path""")
        })

      case "false" if param("resultType", "") != "file" =>
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          var res = ""
          try {
            res = limitOrNot {
              sparkSession.sql(param("sql"))
            }.toJSON
              .collect()
              .mkString(",")
          } catch {
            case e: Exception =>
              e.printStackTrace()
              val msg = if (paramAsBoolean("show_stack", false)) e.getStackTrace.map(f => f.toString).mkString("\n") else ""
              render(500, e.getMessage + "\n" + msg)
          }
          render("[" + res + "]")
        })
    }
  }


  @At(path = Array("/runtime/spark/sql"), types = Array(GET, POST))
  def sql = {
    if (!runtime.isInstanceOf[SparkRuntime]) render(400, "only support spark application")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sparkSession = sparkRuntime.sparkSession
    val tableToPaths = params().filter(f => f._1.startsWith("tableName.")).map(table => (table._1.split("\\.").last, table._2))

    tableToPaths.foreach { tableToPath =>
      val tableName = tableToPath._1
      val loaderClzz = params.filter(f => f._1 == s"loader_clzz.${tableName}").head
      val newParams = params.filter(f => f._1.startsWith(s"loader_param.${tableName}.")).map { f =>
        val coms = f._1.split("\\.")
        val paramStr = coms.takeRight(coms.length - 2).mkString(".")
        (paramStr, f._2)
      }.toMap + loaderClzz

      if (!sparkSession.catalog.tableExists(tableToPath._1) || paramAsBoolean("forceCreateTable", false)) {
        sparkRuntime.operator.createTable(tableToPath._2, tableToPath._1, newParams)
      }
    }

    val sql = if (param("sql").contains(" limit ")) param("sql") else param("sql") + " limit 1000"
    val result = sparkRuntime.operator.runSQL(sql).mkString(",")

    param("resultType", "json") match {
      case "json" => render(200, "[" + result + "]", ViewType.json)
      case _ => renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
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
    val infoMap = StreamingproJobManager.getJobInfo
    render(200, toJsonString(infoMap))
  }

  @Action(
    summary = "show all running stream jobs", description = ""
  )
  @Parameters(Array())
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/stream/jobs/running"), types = Array(GET, POST))
  def streamRunningJobs = {
    setAccessControlAllowOrigin
    val infoMap = runtime.asInstanceOf[SparkRuntime].sparkSession.streams.active.map { f =>
      val startTime = ISODateTimeFormat.dateTime().parseDateTime(f.lastProgress.timestamp).getMillis
      StreamingproJobInfo(null, StreamingproJobType.STREAM, f.name, f.lastProgress.json, f.id + "", startTime, -1l)
    }
    render(200, toJsonString(infoMap))
  }

  @Action(
    summary = "kill specific stream job", description = ""
  )
  @Parameters(Array(
    new Parameter(name = "groupId", required = false, description = "the job id", `type` = "string", allowEmptyValue = false)
  ))
  @Responses(Array(
    new ApiResponse(responseCode = "200", description = "", content = new Content(mediaType = "application/json",
      schema = new Schema(`type` = "string", format = """{}""", description = "")
    ))
  ))
  @At(path = Array("/stream/jobs/kill"), types = Array(GET, POST))
  def killStreamJob = {
    setAccessControlAllowOrigin
    val groupId = param("groupId")
    runtime.asInstanceOf[SparkRuntime].sparkSession.streams.get(groupId).stop()
    render(200, "{}")
  }

  def setAccessControlAllowOrigin = {
    try {
      restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
    } catch {
      case e: RuntimeException =>
    }
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
      val groupIds = StreamingproJobManager.getJobInfo.filter(f => f._2.jobName == jobName).map(f => f._1)
      groupIds.headOption match {
        case Some(groupId) => StreamingproJobManager.killJob(groupId)
        case None =>
      }
    } else {
      StreamingproJobManager.killJob(groupId)
    }

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
      sparkRuntime.closeSession(param("owner", "admin"))
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


  // table create, udf register functions
  // begin -------------------------------------------
  @At(path = Array("/table/create"), types = Array(GET, POST))
  def tableCreate = {
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    if (!runtime.isInstanceOf[SparkRuntime]) render(400, "only support spark application")

    try {
      sparkRuntime.operator.createTable(param("tableName"), param("tableName"), params().toMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        render(e.getMessage)
    }

    render("register success")

  }

  @At(path = Array("/udf"), types = Array(GET, POST))
  def udf = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    sparkSession.sparkContext.addJar(param("path"))
    try {
      val clzz = JarUtil.loadJar(param("path"), param("className"))
      clzz.getMethods.foreach { f =>
        try {
          if (Modifier.isStatic(f.getModifiers)) {
            f.invoke(null, sparkSession.udf)
          }
        } catch {
          case e: Exception =>
            logger.info(s"${f.getName} missing", e)
        }
      }

    } catch {
      case e: Exception =>
        logger.error("udf register fail", e)
        render(400, e.getCause)
    }
    render(200, "[]")
  }

  @At(path = Array("/check"), types = Array(GET, POST))
  def check = {
    render(WowCollections.map())
  }

  @At(path = Array("/test"), types = Array(GET, POST))
  def test = {
    val psDriverBackend = runtime.asInstanceOf[SparkRuntime].psDriverBackend
    val res = psDriverBackend.psDriverRpcEndpointRef.ask[Boolean](Message.CopyModelToLocal(param("hdfs"), param("local")))
    render(s"""{"msg":${res}""")
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
    val session = runtime.asInstanceOf[SparkRuntime].sparkSession
    val resource = new SparkInstanceService(session).resources
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

  private def limitOrNot[T](ds: Dataset[T], maxSize: Int = paramAsInt("maxResultSize", 1000)): Dataset[T] = {
    var result = ds
    if (ds.sparkSession.sparkContext.getConf.getBoolean(MLSQLConf.ENABLE_MAX_RESULT_SIZE.key, false)) {
      val globalLimit = ds.sparkSession.sparkContext.getConf.getInt(
        MLSQLConf.RESTFUL_API_MAX_RESULT_SIZE.key, -1)
      if (globalLimit == -1) {
        result = ds.limit(maxSize)
      } else {
        result = ds.limit(Math.min(globalLimit, maxSize))
      }
    }
    result
  }

  // end --------------------------------------------------------
}
