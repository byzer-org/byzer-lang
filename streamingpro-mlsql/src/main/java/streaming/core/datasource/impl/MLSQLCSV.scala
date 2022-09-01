package streaming.core.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql._
import tech.mlsql.common.form._
import tech.mlsql.tool.HDFSOperatorV2

/**
 * 2019-02-25 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLCSV(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  def handleDFWithOption(originPath: String, config: DataSourceConfig): String = {
    val skipFirstNLines = config.config.get("skipFirstNLines")
    skipFirstNLines match {
      case None => originPath
      case Some(_) => {
        var numsToSkip = skipFirstNLines.get.toInt
        val header = config.config.get("header") match {
          case Some("true") => {
            numsToSkip += 1
            true
          }
          case _ => false
        }
        val encoding = config.config.getOrElse("encoding", "utf-8")
        val path = originPath
        val newPath = HDFSOperatorV2.saveWithoutTopNLines(path, numsToSkip, header, encoding)
        newPath
      }
    }
  }

  def deleteTmpFiles(config: DataSourceConfig, newPath: String): Unit = {
    if (!newPath.equals(config.path) && HDFSOperatorV2.fileExists(newPath)) {
      HDFSOperatorV2.deleteDir(newPath)
    }
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)
    val originPath = resourceRealPath(context.execListener, Option(owner), config.path);
    val newPath = handleDFWithOption(originPath, config)
    val df = reader.options(rewriteConfig(config.config)).format(format).load(newPath)
    df
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "csv"

  override def shortFormat: String = "csv"

  final val header: BooleanParam = new BooleanParam(this, "header",
    FormParams.toJson(Select(
      name = "header",
      values = List(),
      extra = Extra(
        """
          |when set to true the first line of files will be used to name columns and will not be included in data.
          |All types will be assumed string.
          |Default value is false.
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "boolean"
        )), valueProvider = Option(() => {
        List(
          KV(Option("header"), Option("true")),
          KV(Option("header"), Option("false"))
        )
      })
    ))
  )

  setDefault(header, false)


  final val inferSchema: BooleanParam = new BooleanParam(this, "inferSchema",
    FormParams.toJson(Select(
      name = "inferSchema",
      values = List(),
      extra = Extra(
        """
          |automatically infers column types.
          |It requires one extra pass over the data and is false by default
          |""".stripMargin, label = "", options = Map(
          "valueType" -> "boolean"
        )), valueProvider = Option(() => {
        List(
          KV(Option("inferSchema"), Option("true")),
          KV(Option("inferSchema"), Option("false"))
        )
      })
    ))
  )


  setDefault(inferSchema, false)


  final val encoding: Param[String] = new Param[String](this, "encoding",
    FormParams.toJson(Text(
      name = "inferSchema",
      value = "",
      extra = Extra(
        """
          |the csv file encoding. Default utf-8
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        "utf-8"
      })
    ))
  )

  setDefault(encoding, "utf-8")

  final val charToEscapeQuoteEscaping: Param[String] = new Param[String](this, "charToEscapeQuoteEscaping",
    FormParams.toJson(Text(
      name = "charToEscapeQuoteEscaping",
      value = "",
      extra = Extra(
        """
          |Defines the character used to escape the character used for escaping quotes
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        "utf-8"
      })
    ))
  )

  final val delimiter: Param[String] = new Param[String](this, "delimiter",
    FormParams.toJson(Text(
      name = "delimiter",
      value = "",
      extra = Extra(
        """
          |by default columns are delimited using ,, but delimiter can be set to any character
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        ""
      })
    ))
  )


  final val quote: Param[String] = new Param[String](this, "quote",
    FormParams.toJson(Text(
      name = "quote",
      value = "",
      extra = Extra(
        """
          |by default the quote character is ", but can be set to any character. Delimiters inside quotes are ignored
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        ""
      })
    ))
  )


  final val escape: Param[String] = new Param[String](this, "escape",
    FormParams.toJson(Text(
      name = "escape",
      value = "",
      extra = Extra(
        """
          |by default the escape character is \, but can be set to any character. Escaped quote characters are ignored
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        ""
      })
    ))
  )


  final val emptyValue: Param[String] = new Param[String](this, "emptyValue",

    FormParams.toJson(Text(
      name = "emptyValue",
      value = "",
      extra = Extra(
        """
          |String representation of an empty value in read and in write
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        ""
      })
    ))
  )

  final val dateFormat: Param[String] = new Param[String](this, "dateFormat",
    FormParams.toJson(Text(
      name = "dateFormat",
      value = "",
      extra = Extra(
        """
          |specifies a string that indicates the date format to use when reading dates or timestamps. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to both DateType and TimestampType.
          |By default, it is null which means trying to parse times and date by java.sql.Timestamp.valueOf() and java.sql.Date.valueOf().
          |""".stripMargin, label = "", options = Map()), valueProvider = Option(() => {
        ""
      })
    ))
  )


  final val codec: Param[String] = new Param[String](this, "codec",
    FormParams.toJson(Select(
      name = "codec",
      values = List(),
      extra = Extra(
        """
          |For save; compression codec to use when saving to file.
          |Should be the fully qualified name of a class implementing org.apache.hadoop.io.compress.CompressionCodec
          |or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). Defaults to no compression when a codec is not specified.
          |""".stripMargin, label = "", options = Map(
          "stage" -> "save"
        )), valueProvider = Option(() => {
        "bzip2, gzip, lz4,snappy".split(",").map(_.trim).map(item => KV(Option("codec"), Option(item))).toList
      })
    ))
  )
}
