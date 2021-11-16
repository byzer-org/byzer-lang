package tech.mlsql.datasource.impl

import java.nio.charset.Charset

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.client.fluent.{Form, Request}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.form._
import tech.mlsql.common.utils.distribute.socket.server.JavaUtils
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.tool.HDFSOperatorV2

class MLSQLRest(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with DslTool with WowParams {


  def this() = this(BaseParams.randomUID())

  /**
   *
   * load Rest.`http://mlsql.tech/api` where
   * `config.connect-timeout`="10s"
   * and `config.method`="GET"
   * and `header.content-type`="application/json"
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
    val params = config.config
    val httpMethod = params.getOrElse(configMethod.name, "get").toLowerCase
    val request = httpMethod match {
      case "get" => Request.Get(config.path)
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
      request.addHeader(k.stripPrefix("header."), v)
    }

    val response = (httpMethod, params.getOrElse(headerContentType.name, "application/x-www-form-urlencoded") ) match {
      case ("get", _) => request.execute()

      case ("post", "application/json") =>
        if (params.contains(body.name))
          request.bodyString(params(body.name),ContentType.APPLICATION_JSON).execute()
        else
          request.execute()

      case ("post","application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          form.add(k.stripPrefix("form."), v)
        }
        request.bodyForm(form.build(),Charset.forName("utf-8")).execute()

      case (_, v) =>
        throw new MLSQLException(s"content-type ${v}  is not support yet")
    }

    val httpResponse = response.returnResponse()
    val status = httpResponse.getStatusLine.getStatusCode
    val content = EntityUtils.toByteArray(httpResponse.getEntity)
    val session = config.df.get.sparkSession
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
      case "get" => Request.Get(config.path)
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
      request.addHeader(k.stripPrefix("header."), v)
    }

    val response = (httpMethod, params.getOrElse(headerContentType.name, "application/x-www-form-urlencoded")) match {
      case ("get", _) => request.execute()

      case ("post", "application/json") =>
        if (params.contains(body.name))
          request.bodyString(params(body.name),ContentType.APPLICATION_JSON).execute()
        else
          request.execute()

      case ("post", "application/x-www-form-urlencoded") =>
        val form = Form.form()
        params.filter(_._1.startsWith("form.")).foreach { case (k, v) =>
          form.add(k.stripPrefix("form."), v)
        }
        request.bodyForm(form.build(),Charset.forName("utf-8")).execute()

      case ("post", "multipart/form-data") =>

        val context = ScriptSQLExec.contextGetOrForTest()
        val _filePath = params(params("form.file-path"))
        val finalPath = resourceRealPath(context.execListener, Option(context.owner), _filePath)

        val inputStream = HDFSOperatorV2.readAsInputStream(finalPath)

        val fileName = params("form.file-name")

        val entity = MultipartEntityBuilder.create.
          setMode(HttpMultipartMode.BROWSER_COMPATIBLE).
          setCharset(Charset.forName("utf-8")).
          addBinaryBody(fileName, inputStream, ContentType.MULTIPART_FORM_DATA, fileName)

        params.filter(_._1.startsWith("form.")).
          filter(v => v._1 != "form.file-path" && v._1 != "form.file-name").foreach { case (k, v) =>
          entity.addTextBody(k.stripPrefix("form."), v)
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