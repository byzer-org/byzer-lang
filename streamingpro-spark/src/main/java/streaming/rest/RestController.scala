package streaming.rest

import java.util.concurrent.atomic.AtomicInteger

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.sf.json.JSONObject
import streaming.common.SQLContextHolder
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
  * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
  */
class RestController extends ApplicationController with CSVRender {
  @At(path = Array("/runtime/spark/streaming/stop"), types = Array(GET))
  def stopRuntime = {
    runtime.destroyRuntime(true)
    render(200, "ok")
  }

  @At(path = Array("/runtime/spark/sql"), types = Array(GET, POST))
  def sql = {
    if (!runtime.isInstanceOf[SparkRuntime]) render(400, "only support spark application")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sqlContext = SQLContextHolder.getOrCreate.getOrCreate()
    val tableToPaths = params().filter(f => f._1.startsWith("tableName.")).map(table => (table._1.split("\\.").last, table._2))

    tableToPaths.foreach { tableToPath =>
      val tableName = tableToPath._1
      val loaderClzz = params.filter(f => f._1 == s"loader_clzz.${tableName}").head
      val newParams = params.filter(f => f._1.startsWith(s"loader_param.${tableName}.")).map { f =>
        val coms = f._1.split("\\.")
        val paramStr = coms.takeRight(coms.length - 2).mkString(".")
        (paramStr, f._2)
      }.toMap + loaderClzz

      if (!sqlContext.tableNames().contains(tableToPath._1) || paramAsBoolean("forceCreateTable", false)) {
        sparkRuntime.operator.createTable(tableToPath._2, tableToPath._1, newParams)
      }
    }

    val sql = if (param("sql").contains(" limit ")) param("sql") else param("sql") + " limit 1000"
    val result = sparkRuntime.operator.runSQL(sql).mkString(",")

    param("resultType", "html") match {
      case "json" => render(200, "[" + result + "]", ViewType.json)
      case "csv" => renderJsonAsCsv(this.restResponse, "[" + result + "]")
      case _ => renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
    }
  }

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def ddlSql = {
    val sqlContext = SQLContextHolder.getOrCreate.getOrCreate()
    val res = sqlContext.sql(param("sql")).toJSON.collect().mkString(",")
    render("[" + res + "]")
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

  @At(path = Array("/sql"), types = Array(GET, POST))
  def hiveSql = {
    if (!runtime.isInstanceOf[SparkRuntime]) render(400, "only support spark application")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]

    val sql = if (param("sql").contains(" limit ")) param("sql") else param("sql") + " limit 1000"
    val result = sparkRuntime.operator.runSQL(sql).mkString(",")

    param("resultType", "html") match {
      case "json" => render(200, "[" + result + "]", ViewType.json)
      case "csv" => renderJsonAsCsv(this.restResponse, "[" + result + "]")
      case _ => renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
    }
  }


  @At(path = Array("/refresh"), types = Array(GET, POST))
  def refreshTable = {
    Class.forName("org.apache.spark.sql.CarbonContext").getMethod("refreshCache").invoke(SQLContextHolder.getOrCreate.getOrCreate())
    render("SUCCESS", ViewType.string)
  }

  @At(path = Array("/sqlui"), types = Array(GET))
  def sqlui = {
    renderHtml(200, "/rest/sqlui.vm", WowCollections.map())
  }

  @At(path = Array("/index"), types = Array(GET))
  def index = {
    renderHtml(200, "/rest/index.vm", WowCollections.map())
  }

  @At(path = Array("/runtime/spark/streaming/job/add"), types = Array(POST))
  def addJob = {
    if (runtime.isInstanceOf[SparkStreamingRuntime]) render(400, "only support spark streaming application")
    val _runtime = runtime.asInstanceOf[SparkStreamingRuntime]
    val waitCounter = new AtomicInteger(0)
    while (!_runtime.streamingRuntimeInfo.sparkStreamingOperator.isStreamingCanStop()
      && waitCounter.get() < paramAsInt("waitRound", 1000)) {
      Thread.sleep(50)
      waitCounter.incrementAndGet()
    }
    dispatcher(PlatformManager.SPAKR_STREAMING).createStrategy(param("name"), JSONObject.fromObject(request.contentAsString()))
    if (_runtime.streamingRuntimeInfo.sparkStreamingOperator.isStreamingCanStop()) {
      _runtime.destroyRuntime(false)
      new Thread(new Runnable {
        override def run(): Unit = {
          platformManager.run(null, true)
        }

      }).start()

      render(200, "ok")
    } else {
      render(400, "timeout")
    }

  }

  def platformManager = PlatformManager.getOrCreate

  def dispatcher(name: String) = {
    platformManager.findDispatcher
  }

  def runtime = PlatformManager.getRuntime
}
