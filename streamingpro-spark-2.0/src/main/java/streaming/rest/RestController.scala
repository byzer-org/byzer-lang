package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.common.collections.WowCollections
import net.csdn.common.exception.RenderFinish
import net.csdn.modules.http.{ApplicationController, ViewType}
import net.csdn.modules.http.RestRequest.Method._
import org.apache.spark.sql.execution.streaming.{SQLExecute}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime, SparkStructuredStreamingRuntime}
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 28/3/2017.
  */
class RestController extends ApplicationController {
  @At(path = Array("/ss/run/sql"), types = Array(GET, POST))
  def ss = {
    if (!runtime.isInstanceOf[SparkStructuredStreamingRuntime]) {
      render(400, "runtime should be spark_structured_streaming")
    }

    val spark = runtime.asInstanceOf[SparkStructuredStreamingRuntime].sparkSession

    new SQLExecute(spark, restResponse).query(param("sql"))
    throw new RenderFinish()
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

    param("resultType", "html") match {
      case "json" => render(200, "[" + result + "]", ViewType.json)
      case _ => renderHtml(200, "/rest/sqlui-result.vm", WowCollections.map("feeds", result))
    }
  }

  @At(path = Array("/run/sql"), types = Array(GET, POST))
  def ddlSql = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val res = sparkSession.sql(param("sql")).toJSON.collect().mkString(",")
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

  def runtime = PlatformManager.getRuntime
}
