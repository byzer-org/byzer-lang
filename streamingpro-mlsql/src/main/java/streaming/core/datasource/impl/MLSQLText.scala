package streaming.core.datasource.impl

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SaveMode, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.tool.HDFSOperatorV2

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLText(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)

    val paths = config.path.split(",").map { file =>
      resourceRealPath(context.execListener, Option(owner), file)
    }
    val df = reader.options(rewriteConfig(config.config)).format(format).text(paths: _*)

    df.select(F.input_file_name().as("file"), F.col("value"))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val baseDir = resourceRealPath(context.execListener, Option(context.owner), config.path)

    if (HDFSOperatorV2.fileExists(baseDir)) {
      if (config.mode == SaveMode.Overwrite) {
        HDFSOperatorV2.deleteDir(baseDir)
      }
      if (config.mode == SaveMode.ErrorIfExists) {
        throw new MLSQLException(s"${baseDir} is exists")
      }
    }

    config.config.get(contentColumn.name).map { m =>
      set(contentColumn, m)
    }.getOrElse {
      throw new MLSQLException(s"${contentColumn.name} is required")
    }

    config.config.get(fileName.name).map { m =>
      set(fileName, m)
    }.getOrElse {
      throw new MLSQLException(s"${fileName.name} is required")
    }

    val _fileName = $(fileName)
    val _contentColumn = $(contentColumn)

    val saveContent = (fileName: String, buffer: String) => {
      HDFSOperatorV2.saveFile(baseDir, fileName, List(("",buffer)).toIterator)
      baseDir + "/" + fileName
    }

    config.df.get.rdd.map(r => saveContent(r.getAs[String](_fileName), r.getAs[String](_contentColumn))).count()
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)

    val paths = config.path.split(",").map { file =>
      resourceRealPath(context.execListener, Option(owner), file)
    }
    SourceInfo(shortFormat, "", paths.mkString(","))
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "text"

  override def shortFormat: String = fullFormat

  final val wholetext: Param[Boolean] = new Param[Boolean](this, "wholetext", "`wholetext` (default `false`): If true, read a file as a single row and not split by \"\\n\".")
  final val lineSep: Param[String] = new Param[String](this, "lineSep", "`(default covers all `\\r`, `\\r\\n` and `\\n`): defines the line separator\n   * that should be used for parsing.")

  final val contentColumn: Param[String] = new Param[String](this, "contentColumn", "for save mode")
  final val fileName: Param[String] = new Param[String](this, "fileName", "for save mode")
}
