package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource.{DataSinkConfig, DataSourceConfig, MLSQLBaseStreamSource}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLWebConsole(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException(s"load is not support with ${shortFormat} ")
  }

  def isStream = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }


  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    if (isStream) {
      return super.save(batchWriter, config)

    }
    throw new RuntimeException(s"save is not support with ${shortFormat}  in batch mode")

  }

  override def fullFormat: String = "tech.mlsql.stream.sink.MLSQLConsoleSinkProvider"

  override def shortFormat: String = "webConsole"

}

class MLSQLConsole(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException(s"load is not support with ${shortFormat} ")
  }

  def isStream = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.env().contains("streamName")
  }


  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    if (isStream) {
      return super.save(batchWriter, config)

    }
    throw new RuntimeException(s"save is not support with ${shortFormat}  in batch mode")

  }

  override def fullFormat: String = "console"

  override def shortFormat: String = "console"

}


