package tech.mlsql.datasource.impl

import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.UUID

import com.jayway.jsonpath.JsonPath
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
import streaming.rest.RestUtils
import tech.mlsql.common.form._
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.{HDFSOperatorV2, Templates2}

import scala.collection.mutable.ArrayBuffer

class MLSQLRest(override val uid: String) extends MLSQLSource
  with MLSQLSink
  with MLSQLSourceInfo
  with MLSQLSourceConfig
  with MLSQLRegistry with DslTool with WowParams with Logging {


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
   **/
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val skipParams = config.config.getOrElse("config.page.skip-params", "false").toBoolean
    val retryInterval = JavaUtils.timeStringAsMs(config.config.getOrElse("config.retry.interval", "1s"))

    (config.config.get("config.page.next"), config.config.get("config.page.values")) match {
      case (Some(urlTemplate), Some(jsonPath)) =>
        val maxSize = config.config.getOrElse("config.page.limit", "1").toInt
        val maxTries = config.config.getOrElse("config.page.retry", "3").toInt
        val pageInterval = JavaUtils.timeStringAsMs(config.config.getOrElse("config.page.interval", "10ms"))
        var count = 1

        var pageNum = -1
        // if not json path then it should be auto increment page num
        val autoInc = jsonPath.trim.toLowerCase.startsWith("auto-increment")
        if (autoInc) {
          val Array(_, initialPageNum) = jsonPath.split(":")
          pageNum = initialPageNum.toInt
        }


        val (_, firstDfOpt) = RestUtils.executeWithRetrying[(Int, Option[DataFrame])](maxTries)((() => {
          try {
            val tempDF = _http(config.path, config.config, false, config.df.get.sparkSession)
            val row = tempDF.select(F.col("content").cast(StringType), F.col("status")).head
            val status = row.getInt(1)
            (status, Option(tempDF))
          } catch {
            case e: Exception =>
              (500, None)
          }
        }) (),
          tempResp => {
            val t = tempResp._1 == 200
            if (!t) {
              Thread.sleep(retryInterval)
            }
            t
          },
          failResp => logInfo(s"Fail request ${config.path} failed after ${maxTries} attempts. the last response status is: ${failResp._1}. ")
        )

        if (firstDfOpt.isEmpty) {
          throw new MLSQLException(s"Fail request ${config.path} failed after ${maxTries} attempts.")
        }

        var firstDf = firstDfOpt.get

        val uuid = UUID.randomUUID().toString.replaceAll("-", "")
        val context = ScriptSQLExec.context()
        val tmpTablePath = resourceRealPath(context.execListener, Option(context.owner), PathFun("__tmp__").add(uuid).toPath)
        context.execListener.addEnv(classOf[MLSQLRest].getName, tmpTablePath)
        firstDf.write.format("parquet").mode(SaveMode.Append).save(tmpTablePath)

        while (count < maxSize) {
          Thread.sleep(pageInterval)
          count += 1
          val row = firstDf.select(F.col("content").cast(StringType), F.col("status")).head
          val content = row.getString(0)

          val pageValues = if (autoInc) {
            try {
              jsonPath.split(",").map(path => JsonPath.read[String](content, path)).toArray
            } catch {
              case _: com.jayway.jsonpath.PathNotFoundException =>
                Array[String]()
              case e: Exception =>
                throw e
            }
          } else Array(pageNum.toString)


          val shouldStop = pageValues.size == 0 || pageValues.filter(value => value == null || value.isEmpty).size > 0
          if (shouldStop) {
            count = maxSize
          } else {
            val newUrl = Templates2.evaluate(urlTemplate, pageValues)
            RestUtils.executeWithRetrying[(Int, Option[DataFrame])](maxTries)((() => {
              try {
                val tempDF = _http(newUrl, config.config, skipParams, config.df.get.sparkSession)
                val row = tempDF.select(F.col("content").cast(StringType), F.col("status")).head
                firstDf = tempDF
                val status = row.getInt(1)
                (status, Option(tempDF))
              } catch {
                case e: Exception =>
                  (500, None)
              }
            }) (),
              tempResp => {
                val t = tempResp._1 == 200
                if (!t) {
                  Thread.sleep(retryInterval)
                }
                t
              },
              failResp => logInfo(s"Fail request ${newUrl} failed after ${maxTries} attempts. the last response status is: ${failResp._1}. ")
            )
            firstDf.write.format("parquet").mode(SaveMode.Append).save(tmpTablePath)
            pageNum += 1
          }

        }
        context.execListener.sparkSession.read.parquet(tmpTablePath)

      case (None, None) =>

        val maxTries = config.config.getOrElse("config.retry", config.config.getOrElse("config.page.retry", "3")).toInt


        val (_, resultDF) = RestUtils.executeWithRetrying[(Int, Option[DataFrame])](maxTries)((() => {
          try {
            val tempDF = _http(config.path, config.config, false, config.df.get.sparkSession)
            val row = tempDF.select(F.col("content").cast(StringType), F.col("status")).head
            val status = row.getInt(1)
            (status, Option(tempDF))
          } catch {
            case e: Exception =>
              (500, None)
          }
        }) (),
          tempResp => {
            val t = tempResp._1 == 200
            if (!t) {
              Thread.sleep(retryInterval)
            }
            t
          },
          failResp => logInfo(s"Fail request ${config.path} failed after ${maxTries} attempts. the last response status is: ${failResp._1}. ")
        )
        if (resultDF.isEmpty) {
          val session = ScriptSQLExec.context().execListener.sparkSession
          return session.createDataFrame(session.sparkContext.makeRDD(Seq(Row.fromSeq(Seq(Array[Byte](), 0))))
            , StructType(fields = Seq(
              StructField("content", BinaryType), StructField("status", IntegerType)
            )))
        }
        resultDF.get
    }


  }

  override def skipDynamicEvaluation = true


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
            url + "&" + urlParam
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

      case ("post", contentType) if contentType.trim.startsWith("application/json") =>
        if (params.contains(body.name)) {
          request.bodyString(params(body.name), ContentType.APPLICATION_JSON).execute()
        } else {
          request.execute()
        }

      case ("post", contentType) if contentType.trim.startsWith("application/x-www-form-urlencoded") =>
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
            config.path + urlParam
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

        val context = ScriptSQLExec.contextGetOrForTest()
        val _filePath = params("form.file-path")
        val finalPath = resourceRealPath(context.execListener, Option(context.owner), _filePath)

        val inputStream = HDFSOperatorV2.readAsInputStream(finalPath)

        val fileName = params("form.file-name")

        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8")).
          addBinaryBody(fileName, inputStream, ContentType.MULTIPART_FORM_DATA, fileName)

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