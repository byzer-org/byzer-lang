package streaming.core.datasource.impl

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

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
    throw new RuntimeException("text not support save")
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
}
