package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-04-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDelta(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  override def shortFormat: String = "delta"

}

class MLSQLRate(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val streamReader = config.df.get.sparkSession.readStream
    val format = config.config.getOrElse("implClass", fullFormat)
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    streamReader.options(config.config).format(format).load(resolvePath(config.path, owner))
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    return super.save(batchWriter, config)

  }


  override def resolvePath(path: String, owner: String): String = {
    val context = ScriptSQLExec.contextGetOrForTest()
    resourceRealPath(context.execListener, Option(owner), path)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  override def shortFormat: String = "rate"

}
