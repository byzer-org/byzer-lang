package streaming.rest

import java.lang.reflect.Modifier
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode}
import streaming.common.JarUtil
import streaming.core._
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib.algs.tf.cluster.{ClusterSpec, ClusterStatus, ExecutorInfo}
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

/**
  * Created by allwefantasy on 28/3/2017.
  */
class RestController extends ApplicationController {

  // mlsql script execute api, support async and sysn
  // begin -------------------------------------------
  @At(path = Array("/run/script"), types = Array(GET, POST))
  def script = {
    restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
    val silence = paramAsBoolean("silence", false)
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
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
            val allPathPrefix = fromJson(param("allPathPrefix"), classOf[Map[String, String]])
            val defaultPathPrefix = param("defaultPathPrefix")
            ScriptSQLExec.parse(param("sql"), new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix))
            htp.get(new Url(param("callback")), Map("stat" -> s"""success"""))
          } catch {
            case e: Exception =>
              e.printStackTrace()
              htp.get(new Url(param("callback")), Map("fail" -> s"""success"""))
          }
        })
      } else {
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          val allPathPrefix = fromJson(param("allPathPrefix", "{}"), classOf[Map[String, String]])
          val defaultPathPrefix = param("defaultPathPrefix", "")
          val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix)
          ScriptSQLExec.parse(param("sql"), context)
          if (!silence) {
            outputResult = context.getLastSelectTable() match {
              case Some(table) => "[" + sparkSession.sql(s"select * from $table limit 100").toJSON.collect().mkString(",") + "]"
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

  // download hdfs file
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
          val dfWriter = sparkSession.sql(param("sql")).write.mode(SaveMode.Overwrite)
          _save(dfWriter)
          val htp = findService(classOf[HttpTransportService])
          import scala.collection.JavaConversions._
          htp.get(new Url(param("callback")), Map("url_path" -> s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path"""))
        })
        render("[]")

      case "false" if param("resultType", "") == "file" =>
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          val dfWriter = sparkSession.sql(param("sql")).write.mode(SaveMode.Overwrite)
          _save(dfWriter)
          render(s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path""")
        })

      case "false" if param("resultType", "") != "file" =>
        StreamingproJobManager.run(sparkSession, jobInfo, () => {
          var res = ""
          try {
            res = sparkSession.sql(param("sql")).toJSON.collect().mkString(",")
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
  @At(path = Array("/runningjobs"), types = Array(GET, POST))
  def getRunningJobGroup = {
    val infoMap = StreamingproJobManager.getJobInfo
    render(200, toJsonString(infoMap))
  }

  @At(path = Array("/stream/jobs/running"), types = Array(GET, POST))
  def streamRunningJobs = {
    restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
    val infoMap = runtime.asInstanceOf[SparkRuntime].sparkSession.streams.active.map { f =>
      StreamingproJobInfo(null, StreamingproJobType.STREAM, f.name, f.lastProgress.json, f.id + "", -1l, -1l)
    }
    render(200, toJsonString(infoMap))
  }

  @At(path = Array("/stream/jobs/kill"), types = Array(GET, POST))
  def killStreamJob = {
    restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
    val groupId = param("groupId")
    runtime.asInstanceOf[SparkRuntime].sparkSession.streams.get(groupId).stop()
    render(200, "{}")
  }

  @At(path = Array("/killjob"), types = Array(GET, POST))
  def killJob = {
    restResponse.httpServletResponse().setHeader("Access-Control-Allow-Origin", "*")
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
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    render(sparkSession.table(param("name")))
  }

  @At(path = Array("/test"), types = Array(GET, POST))
  def test = {
    val psDriverBackend = runtime.asInstanceOf[SparkRuntime].psDriverBackend
    psDriverBackend.psDriverRpcEndpointRef.send(Message.TensorFlowModelClean("/tmp/ok"))
    render("{}")
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

  // end --------------------------------------------------------
}
