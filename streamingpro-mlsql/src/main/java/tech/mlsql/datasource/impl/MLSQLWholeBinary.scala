package tech.mlsql.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._

/**
  * 2019-06-19 weicm
  * Whole binary datasource can load hdfs files as a table with two columns(path:string, content:binary) and save table as hdfs files!
  * mlsql eg: load wholeBinary./tmp/test/ as output;
  */
class MLSQLWholeBinary(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)

    val paths = config.path.split(",").map { file =>
      resourceRealPath(context.execListener, Option(owner), file)
    }
    reader.options(rewriteConfig(config.config)).format(format).load(paths: _*)
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val baseDir = resourceRealPath(context.execListener, Option(context.owner), config.path)
    val format = config.config.getOrElse("implClass", fullFormat)
    writer.options(rewriteConfig(config.config)).mode(config.mode).format(format).save(baseDir)
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

  override def fullFormat: String = "org.apache.spark.sql.execution.datasources.binary.WholeBinaryFileFormat"

  override def shortFormat: String = "wholeBinary"

  final val contentColumn: Param[String] = new Param[String](this, "contentColumn", "for save mode")
  final val fileName: Param[String] = new Param[String](this, "fileName", "for save mode")
}
