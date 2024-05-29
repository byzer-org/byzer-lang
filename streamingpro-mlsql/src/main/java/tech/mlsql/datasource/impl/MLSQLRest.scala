package tech.mlsql.datasource.impl

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SaveMode, SparkSession, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.log.WowLog
import tech.mlsql.common.form._
import tech.mlsql.common.utils.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.crawler.RestUtils.executeWithRetrying
import tech.mlsql.datasource.helper.rest.PageStrategyDispatcher
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.{HDFSOperatorV2, Templates2}

import java.net.URLEncoder
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object MLSQLRestCache extends Logging {
  val cacheFiles = CacheBuilder.newBuilder().
    maximumSize(100000).removalListener(new RemovalListener[String, java.util.List[String]]() {
      override def onRemoval(notification: RemovalNotification[String, util.List[String]]): Unit = {
        val files = notification.getValue
        if (files != null) {
          val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
          files.asScala.foreach { file =>
            try {
              logInfo(s"remove cache file ${file}")
              fs.delete(new Path(file), true)
            } catch {
              case e: Exception =>
                logError(s"remove cache file ${file} failed", e)
            }
          }
        }
      }
    }).
    expireAfterWrite(1, TimeUnit.DAYS).
    build[String, java.util.List[String]]()
}

class MLSQLRest(override val uid: String) extends MLSQLSource
  with MLSQLSink
  with MLSQLSourceInfo
  with MLSQLSourceConfig
  with MLSQLRegistry with DslTool with WowParams with Logging with WowLog {


  def this() = this(BaseParams.randomUID())

  /**
   *
   * load Rest.`http://mlsql.tech/api` where
   * `config.connect-timeout`="10s"
   * and `config.method`="GET"
   * and `config.retry`="3"
   * and `header.content-type`="application/json"
   *
   * and `config.page.next`="http://mlsql.tech/api?cursor={0}&wow={1}"
   * and `config.page.skip-params`="true"
   * and `config.page.values`="$.path,$.path2" -- json path
   * and `config.page.interval`="10ms"
   * and `config.page.retry`="3"
   * and `config.page.limit`="100"
   *
   * and `body`='''
   * {
   * "query":"b"
   * }
   * '''
   * -- and `form.query`="b"
   * as apiTable;
   *
   * -- apiTable schema: schema(field(content,binary),field(contentType,string),field(status,int))
   *
   * select cast(content as string) as content from apiTable as newTable;
   *
   * -- suppose it's json array
   * select explode(to_json_array(content)) as jsonColumn from newTable as jsonTable;
   *
   * -- expand json field as a real multi-columns table
   * run jsonTable AS JsonExpandExt.`` WHERE inputCol="jsonColumn" AS table_3;
   *
   * select * from table_3 as output;
   * */
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val skipParams = config.config.getOrElse("config.page.skip-params", "false").toBoolean
    val retryInterval = JavaUtils.timeStringAsMs(config.config.getOrElse("config.retry.interval", "1s"))
    val debug = config.config.getOrElse("config.debug", "false").toBoolean
    val strategy = config.config.getOrElse("config.page.error.strategy", "abort")
    val enableRequestCleaner = config.config.getOrElse(configEnableRequestCleaner.name, "false")
    ScriptSQLExec.context().execListener.addEnv("enableRestDataSourceRequestCleaner", enableRequestCleaner)

    def _error2DataFrame(errMsg: String, statusCode: Int, session: SparkSession): DataFrame = {
      session.createDataFrame(session.sparkContext.makeRDD(Seq(Row.fromSeq(Seq(errMsg.getBytes(UTF_8), statusCode))))
        , StructType(fields = Seq(
          StructField("content", BinaryType), StructField("status", IntegerType)
        )))
    }

    /**
     * Calling http rest endpoints with retrying
     *
     * @param url        http url
     * @param skipParams if true, not adding parameters to http get urls
     * @maxTries The max number of attempts
     * @return http status code along with DataFrame
     */
    def _httpWithRetrying(url: String, skipParams: Boolean, maxTries: Int): (Int, DataFrame) = {
      executeWithRetrying[(Int, DataFrame)](maxTries)((() => {
        try {
          if (debug) {
            logInfo(format(s"Started to request Page ${url} "))
          }
          val df = _http(url, config.config, skipParams, config.df.get.sparkSession)
          val row = df.select(F.col("content").cast(StringType), F.col("status")).head
          val status = row.getInt(1)
          (status, df)
        } catch {
          // According to _http function, it throws MLSQLException if any request parameter is invalid
          case me: MLSQLException =>
            if (me.getMessage.startsWith("content-type"))
              (415, _error2DataFrame(me.getMessage, 415, config.df.get.sparkSession))
            else if (me.getMessage.startsWith("HTTP method"))
              (405, _error2DataFrame(me.getMessage, 405, config.df.get.sparkSession))
            else
              (500, _error2DataFrame(me.getMessage, 500, config.df.get.sparkSession))
          case e: Exception => (0, _error2DataFrame(e.getMessage, 0, config.df.get.sparkSession))
        }
      })(),
        tempResp => {
          val succeed = tempResp._1 == 200
          if (!succeed) {
            Thread.sleep(retryInterval)
          }
          succeed
        },
        failResp => {
          logInfo(s"Request ${url} failed after ${maxTries} attempts. the last response status is: ${failResp._1}. ")
        }
      )
    }

    (config.config.get("config.page.next"), config.config.get("config.page.values")) match {
      // Multiple-page request
      case (Some(_), Some(_)) =>

        // max page size
        val maxSize = config.config.getOrElse("config.page.limit", "1").toInt
        // sleep between each page request
        val pageInterval = JavaUtils.timeStringAsMs(config.config.getOrElse("config.page.interval", "10ms"))
        val pageStrategy = PageStrategyDispatcher.get(config.config)
        // Max number of attempt for each page
        val maxTries = config.config.getOrElse("config.page.retry", "3").toInt

        val uuid = UUID.randomUUID().toString.replaceAll("-", "")
        val context = ScriptSQLExec.context()
        // each page's result is saved in tmpTablePath
        val tmpTablePath = resourceRealPath(context.execListener, Option(context.owner),
          PathFun("__tmp__").add("rest").add(uuid).toPath)

        val cachedFiles = MLSQLRestCache.cacheFiles.get(config.path, new java.util.concurrent.Callable[java.util.List[String]] {
          override def call(): java.util.List[String] = {
            new util.LinkedList[String]()
          }
        })
        cachedFiles.add(tmpTablePath)

        // If a user runs multiple Load Rest.`` statements in a single thread, we need to save all temp dirs
        context.execListener.env().get(classOf[MLSQLRest].getName) match {
          case Some(dirs) => context.execListener.addEnv(classOf[MLSQLRest].getName, s"${dirs},${tmpTablePath}")
          case None => context.execListener.addEnv(classOf[MLSQLRest].getName, tmpTablePath)
        }

        var count = 0
        var status: Int = 0
        var hasNextPage: Boolean = false
        var url = config.path
        do {
          val pageFetchTime = System.currentTimeMillis()
          val _skipParams = if (count == 0) false else skipParams
          val (_, dataFrame) = _httpWithRetrying(url, _skipParams, maxTries)
          val row = dataFrame.select(F.col("content").cast(StringType), F.col("status")).head
          val content = row.getString(0)
          // Reset status
          status = row.getInt(1)
          pageStrategy.nexPage(Option(content))
          url = pageStrategy.pageUrl(Option(content))
          hasNextPage = pageStrategy.hasNextPage(Option(content))
          val exceptionMsg = s"URL:${url},with response status ${status}, Have retried ${maxTries} times!\n ${content}"
          if (status != 200) {
            // if strategy is abort, then the raise error, else if the strategy is skip, just log warn the exception
            strategy match {
              case "abort" => throw new RuntimeException(exceptionMsg)
              case "skip" =>
                // First page request failed, return immediately, otherwise return saved DataFrame
                logWarning(exceptionMsg)
                if (count == 0) return dataFrame else return context.execListener.sparkSession.read.parquet(tmpTablePath)
            }

          }
          dataFrame.write.format("parquet").mode(SaveMode.Append).save(tmpTablePath)
          logInfo(s"Data from url:${url} from start:${count * maxSize} to end:${(count + 1) * maxSize - 1} " +
            s"is done! and dump to ${tmpTablePath}")
          if (debug) {
            logInfo(format(s"Getting Page ${count} ${url} Consume:${System.currentTimeMillis() - pageFetchTime}ms"))
          }
          if (count > 0) Thread.sleep(pageInterval)
          count += 1
        }
        while (count < maxSize && hasNextPage && status == 200)
        context.execListener.sparkSession.read.parquet(tmpTablePath)

      case (None, None) =>
        // One page http request
        val maxTries = config.config.getOrElse("config.retry", config.config.getOrElse("config.page.retry", "3")).toInt
        _httpWithRetrying(config.path, skipParams, maxTries)._2
    }
  }

  override def skipDynamicEvaluation = true

  /**
   * Send http request and return the result as a DataFrame
   *
   * @param url        http url
   * @param params     http parameters, including content-type http method and others
   * @param skipParams if true, not adding parameters to http get url
   * @param session    The Spark Session
   * @return DataFrame , is guaranteed to be not null, with 2 columns content: Array[String] status: Int
   */
  private def _http(url: String, params: Map[String, String], skipParams: Boolean, session: SparkSession): DataFrame = {
    val httpMethod = params.getOrElse(configMethod.name, "get").toLowerCase
    val request = httpMethod match {
      case "get" =>

        val paramsBuf = ArrayBuffer[(String, String)]()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          paramsBuf.append((k.stripPrefix("form."), Templates2.dynamicEvaluateExpression(URLEncoder.encode(v, "utf-8"), ScriptSQLExec.context().execListener.env().toMap)))
        }

        val finalUrl = if (paramsBuf.length > 0 && !skipParams) {
          val urlParam = paramsBuf.map { case (k, v) => s"${k}=${v}" }.mkString("&")
          if (url.contains("?")) {
            if (url.endsWith("?")) url + urlParam else url + "&" + urlParam
          } else {
            url + "?" + urlParam
          }
        } else url
        Request.Get(finalUrl)

      case "post" => Request.Post(url)
      case "put" => Request.Put(url)
      case v => throw new MLSQLException(s"HTTP method ${v} is not support yet")
    }

    if (params.contains(configSocketTimeout.name)) {
      request.socketTimeout(JavaUtils.timeStringAsMs(params(configSocketTimeout.name)).toInt)
    }

    if (params.contains(configConnectTimeout.name)) {
      request.connectTimeout(JavaUtils.timeStringAsMs(params(configConnectTimeout.name)).toInt)
    }

    params.filter(_._1.startsWith("header.")).foreach { case (k, v) =>
      request.setHeader(k.stripPrefix("header."), v)
    }
    val contentTypeValue = params.getOrElse(headerContentType.name,
      params.getOrElse("header.Content-Type", "application/x-www-form-urlencoded"))
    request.setHeader("Content-Type", contentTypeValue)

    val response = (httpMethod, contentTypeValue) match {
      case ("get", _) =>
        request.execute()

      case ("post" | "put", contentType) if contentType.trim.startsWith("application/json") =>
        if (params.contains(body.name)) {
          request.bodyString(params(body.name), ContentType.APPLICATION_JSON).execute()
        } else {
          request.execute()
        }

      case ("post" | "put", contentType) if contentType.trim.startsWith("application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          form.add(k.stripPrefix("form."), Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.bodyForm(form.build(), Charset.forName("utf-8")).execute()

      case (_, v) =>
        throw new MLSQLException(s"content-type ${v}  is not support yet")
    }

    val httpResponse = response.returnResponse()
    val status = httpResponse.getStatusLine.getStatusCode
    val content = if (httpResponse.getEntity != null) EntityUtils.toByteArray(httpResponse.getEntity) else Array[Byte]()
    logInfo(format(s"$url $status"))
    session.createDataFrame(session.sparkContext.makeRDD(Seq(Row.fromSeq(Seq(content, status))))
      , StructType(fields = Seq(
        StructField("content", BinaryType), StructField("status", IntegerType)
      )))
  }


  /**
   * -- throw exception when save fails (http status != 200)
   * save overwrite table Rest.`http://mlsql.tech/api` where
   * `config.connect-timeout`="10s"
   * and `config.method`="POST"
   * and `header.content-type`="application/json"
   * and `body`='''
   * {
   * "query":"b"
   * }
   * ''';
   */
  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    val params = config.config
    val httpMethod = params.getOrElse(configMethod.name, "get").toLowerCase
    val request = httpMethod match {
      case "get" =>
        val paramsBuf = ArrayBuffer[(String, String)]()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          paramsBuf.append((k.stripPrefix("form."), Templates2.dynamicEvaluateExpression(URLEncoder.encode(v, "utf-8"), ScriptSQLExec.context().execListener.env().toMap)))
        }

        val finalUrl = if (paramsBuf.length > 0) {
          val urlParam = paramsBuf.map { case (k, v) => s"${k}=${v}" }.mkString("&")
          if (config.path.contains("?")) {
            if (config.path.endsWith("?")) config.path + urlParam else config.path + "&" + urlParam
          } else {
            config.path + "?" + urlParam
          }
        } else config.path

        Request.Get(finalUrl)

      case "post" => Request.Post(config.path)
      case "put" => Request.Put(config.path)
      case v => throw new MLSQLException(s"HTTP method ${v} is not support yet")
    }

    if (params.contains(configSocketTimeout.name)) {
      request.socketTimeout(JavaUtils.timeStringAsMs(params(configSocketTimeout.name)).toInt)
    }

    if (params.contains(configConnectTimeout.name)) {
      request.connectTimeout(JavaUtils.timeStringAsMs(params(configConnectTimeout.name)).toInt)
    }

    params.filter(_._1.startsWith("header.")).foreach { case (k, v) =>
      request.setHeader(k.stripPrefix("header."), v)
    }
    val contentTypeValue = params.getOrElse(headerContentType.name,
      params.getOrElse("header.Content-Type", "application/x-www-form-urlencoded"))
    request.setHeader("Content-Type", contentTypeValue)

    val response = (httpMethod, contentTypeValue) match {
      case ("get", _) => request.execute()

      case ("post", contentType) if contentType.trim.startsWith("application/json") =>
        if (params.contains(body.name))
          request.bodyString(params(body.name), ContentType.APPLICATION_JSON).execute()
        else {
          request.execute()
        }


      case ("post", contentType) if contentType.trim.startsWith("application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          form.add(k.stripPrefix("form."), Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
        }
        request.bodyForm(form.build(), Charset.forName("utf-8")).execute()

      case ("post", contentType) if contentType.trim.startsWith("multipart/form-data") =>
        // Avoid setting boundary
        request.removeHeaders("Content-Type")

        val context = ScriptSQLExec.contextGetOrForTest()
        val _filePath = params("form.file-path")
        val finalPath = resourceRealPath(context.execListener, Option(context.owner), _filePath)

        val fileName = params("form.file-name")

        val filePathBuf = ArrayBuffer[(String, String)]()
        HDFSOperatorV2.isFile(finalPath) match {
          case true =>
            filePathBuf.append((finalPath, fileName))
          case false if HDFSOperatorV2.isDir(finalPath) =>
            val listFiles = HDFSOperatorV2.listFiles(finalPath)
            if (listFiles.filter(_.isDirectory).size > 0) throw new MLSQLException(s"Including subdirectories is not supported")

            listFiles.filterNot(_.getPath.getName.equals("_SUCCESS"))
              .foreach(file => filePathBuf.append((file.getPath.toString, file.getPath.getName)))
          case _ =>
            throw new MLSQLException(s"Please check whether the specified directory or file exists")
        }

        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8"))

        filePathBuf.map(fileInfo => {
          val inputStream = HDFSOperatorV2.readAsInputStream(fileInfo._1)
          entity.addBinaryBody(fileInfo._2, inputStream, ContentType.MULTIPART_FORM_DATA, fileInfo._2)
        })

        params.filter(_._1.startsWith("form.")).
          filter(v => v._1 != "form.file-path" && v._1 != "form.file-name").foreach { case (k, v) =>
            entity.addTextBody(k.stripPrefix("form."), Templates2.dynamicEvaluateExpression(v, ScriptSQLExec.context().execListener.env().toMap))
          }
        request.body(entity.build()).execute()

      case (_, v) =>
        throw new MLSQLException(s"content-type ${v}  is not support yet")
    }

    val httpResponse = response.returnResponse()
    val status = httpResponse.getStatusLine.getStatusCode
    EntityUtils.consumeQuietly(httpResponse.getEntity)
    if (status != 200) {
      throw new RuntimeException(s"Fail to save ${config.path}")
    }
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    SourceInfo("rest", "http", config.path)
  }


  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "Rest"

  override def shortFormat: String = "Rest"

  final val configEnableRequestCleaner = new Param[String](parent = this, name = "config.enableRequestCleaner",
    FormParams.toJson(Select(
      name = "config.enableRequestCleaner",
      values = List(),
      extra = Extra(
        doc =
          """
            | Enable temp data cleaner
          """,
        label = "enableRequestCleaner",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(
          KV(Option("config.enableRequestCleaner"), Option("true")),
          KV(Option("config.enableRequestCleaner"), Option("false"))
        )
      })
    )
    )
  )

  final val configMethod = new Param[String](this, "config.method",
    FormParams.toJson(Select(
      name = "config.method",
      values = List(),
      extra = Extra(
        doc =
          """
            | http method
          """,
        label = "http mehod",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(
          KV(Option("config.method"), Option("get")),
          KV(Option("config.method"), Option("post")),
          KV(Option("config.method"), Option("put")),
          KV(Option("config.method"), Option("delete"))
        )
      })
    )
    ))

  final val configConnectTimeout = new Param[String](this, "config.connect-timeout",
    FormParams.toJson(Text(
      name = "config.connect-timeout",
      value = "",
      extra = Extra(
        doc =
          """
            |  connect-timeout
          """,
        label = "config.connect-timeout",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val configSocketTimeout = new Param[String](this, "config.socket-timeout",
    FormParams.toJson(Text(
      name = "config.socket-timeout",
      value = "",
      extra = Extra(
        doc =
          """
            |  config.socket-timeout
          """,
        label = "config.socket-timeout",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )


  final val formFileName: Param[String] = new Param[String](this, "form.file-name",
    FormParams.toJson(Text(
      name = "form.file-name",
      value = "",
      extra = Extra(
        doc =
          """
            | upload file name
          """,
        label = "form.file-name",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val formFilePath: Param[String] = new Param[String](this, "form.file-path",
    FormParams.toJson(Text(
      name = "form.file-path",
      value = "",
      extra = Extra(
        doc =
          """
            | upload file path
          """,
        label = "form.file-path",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )

  final val formRest: Param[String] = new Param[String](this, "form.*",
    FormParams.toJson(Text(
      name = "form.*",
      value = "",
      extra = Extra(
        doc =
          """
            | form.*
          """,
        label = "form.*",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )


  final val body: Param[String] = new Param[String](this, "body",
    FormParams.toJson(Text(
      name = "body",
      value = "",
      extra = Extra(
        doc =
          """
            | json string
          """,
        label = "body",
        options = Map(
          "valueType" -> "string",
          "defaultValue" -> "",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )


  final val headerContentType: Param[String] = new Param[String](this, "header.content-type",
    FormParams.toJson(Select(
      name = "header.content-type",
      values = List(),
      extra = Extra(
        doc =
          """
            | http request content type
          """,
        label = "header.content-type",
        options = Map(
          "valueType" -> "string",
          "required" -> "false",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        List(
          KV(Option("header.content-type"), Option("application/json")),
          KV(Option("header.content-type"), Option("application/x-www-form-urlencoded")),
          KV(Option("header.content-type"), Option("application/octet-stream")),
          KV(Option("header.content-type"), Option("multipart/form-data"))
        )
      })
    )
    ))


}