package streaming.rest

import java.lang.reflect.Modifier

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.common.path.Url
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.transport.HttpTransportService
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode}
import streaming.common.JarUtil
import streaming.core.{AsyncJobRunner, DownloadRunner, JobCanceller}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 28/3/2017.
  */
class RestController extends ApplicationController {

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

    param("resultType", "html") match {
      case "json" => render(200, "[" + result + "]", ViewType.json)
      case _ => renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
    }
  }

  def _save(dfWriter: DataFrameWriter[Row]) = {
    if (hasParam("tableName")) {
      dfWriter.saveAsTable(param("tableName"))
    } else {
      dfWriter.format(param("format", "csv")).save(param("path", "-"))
    }
  }


  @At(path = Array("/run/script"), types = Array(GET, POST))
  def script = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val htp = findService(classOf[HttpTransportService])
    if (paramAsBoolean("async", false) && !params().containsKey("callback")) {
      render(400, "when async is set true ,then you should set callback url")
    }
    try {
      if (paramAsBoolean("async", false)) {
        AsyncJobRunner.run(() => {
          try {
            ScriptSQLExec.parse(param("sql"), new ScriptSQLExecListener(sparkSession, param("prefixPath")))
            htp.get(new Url(param("callback")), Map("stat" -> s"""success"""))
          } catch {
            case e: Exception =>
              e.printStackTrace()
              htp.get(new Url(param("callback")), Map("fail" -> s"""success"""))
          }
        })
      } else {
        ScriptSQLExec.parse(param("sql"), new ScriptSQLExecListener(sparkSession, param("prefixPath")))
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        val msg = if (paramAsBoolean("show_stack", false)) e.getStackTrace.map(f => f.toString).mkString("\n") else ""
        render(500, e.getMessage + "\n" + msg)
    }
    render(200, WowCollections.map())
  }

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def ddlSql = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val path = param("path", "-")
    if (paramAsBoolean("async", false) && path == "-") {
      render(s"""path should not be empty""")
    }

    paramAsBoolean("async", false).toString match {
      case "true" if hasParam("callback") =>
        AsyncJobRunner.run(() => {
          val dfWriter = sparkSession.sql(param("sql")).write.mode(SaveMode.Overwrite)
          _save(dfWriter)
          val htp = findService(classOf[HttpTransportService])
          import scala.collection.JavaConversions._
          htp.get(new Url(param("callback")), Map("url_path" -> s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path"""))
        })
        render("[]")

      case "false" if param("resultType", "") == "file" =>
        JobCanceller.runWithGroup(sparkSession.sparkContext, paramAsLong("timeout", 30000), () => {
          val dfWriter = sparkSession.sql(param("sql")).write.mode(SaveMode.Overwrite)
          _save(dfWriter)
          render(s"""/download?fileType=raw&file_suffix=${param("format", "csv")}&paths=$path""")
        })

      case "false" if param("resultType", "") != "file" =>
        JobCanceller.runWithGroup(sparkSession.sparkContext, paramAsLong("timeout", 30000), () => {
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

  @At(path = Array("/stat"), types = Array(GET, POST))
  def stat = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    sparkSession.sparkContext
    render()
  }

  @At(path = Array("/download"), types = Array(GET, POST))
  def download = {
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

  def runtime = PlatformManager.getRuntime
}
