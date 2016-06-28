package streaming.rest

import java.util.concurrent.atomic.AtomicInteger

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.modules.http.RestRequest.Method._
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.sf.json.JSONObject
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
 * 4/30/16 WilliamZhu(allwefantasy@gmail.com)
 */
class RestController extends ApplicationController {
  @At(path = Array("/runtime/spark/streaming/stop"), types = Array(GET))
  def stopRuntime = {
    runtime.destroyRuntime(true)
    render(200, "ok")
  }

  @At(path = Array("/runtime/spark/sql"), types = Array(POST))
  def sql = {
    if (!runtime.isInstanceOf[SparkRuntime]) render(400, "only support spark application")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val tableToPaths = params().filter(f => f._1.startsWith("tableName.")).map(table => (table._1.split("\\.").last, table._2))
    tableToPaths.foreach { tableToPath =>
      val tableName = tableToPath._1
      val loaderClzz = params.filter(f => f._1 == s"loader_clzz.${tableName}").head
      val newParams = params.filter(f => f._1.startsWith(s"loader_param.${tableName}.")).map { f =>
        val coms = f._1.split("\\.")
        val paramStr = coms.takeRight(coms.length - 2).mkString(".")
        (paramStr, f._2)
      }.toMap + loaderClzz
      sparkRuntime.operator.createTable(tableToPath._2, tableToPath._1, newParams)
    }

    val result = sparkRuntime.operator.runSQL(param("sql")).mkString(",")
    if(param("resultType","html")=="json")
      render(200, result, ViewType.json)
    else
      renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
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
