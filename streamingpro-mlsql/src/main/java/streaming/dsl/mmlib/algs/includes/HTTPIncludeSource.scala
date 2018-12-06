package streaming.dsl.mmlib.algs.includes

import org.apache.spark.sql.SparkSession
import streaming.crawler.HttpClientCrawler
import streaming.dsl.{IncludeSource, ScriptSQLExec}
import streaming.log.Logging

/**
  * Created by allwefantasy on 30/8/2018.
  */
class HTTPIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {

    val context = ScriptSQLExec.context()

    var params = scala.collection.mutable.HashMap(
      "path" -> path
    )
    if (context.owner != null) {
      params += ("owner" -> context.owner)
    }

    options.filter(f => f._1.startsWith("param.")).map(f => (f._1.substring("param.".length), f._2)).foreach { f =>
      params += f
    }

    val method = options.getOrElse("method", "get")
    val fetch_url = context.userDefinedParam.getOrElse("__default__include_fetch_url__", path)
    val projectName = context.userDefinedParam.getOrElse("__default__include_project_name__", null)

    if (projectName != null) {
      params += ("projectName" -> projectName)
    }

    logInfo(s"""HTTPIncludeSource URL: ${fetch_url}  PARAMS:${params.map(f => s"${f._1}=>${f._2}").mkString(";")}""")
    HttpClientCrawler.requestByMethod(fetch_url, method, params.toMap)
  }

  override def skipPathPrefix: Boolean = true
}
