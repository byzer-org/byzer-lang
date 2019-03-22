package streaming.core.datasource.impl

import net.sf.json.JSONObject
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLScript(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val spark = config.df.get.sparkSession
    val items = List(cleanBlockStr(context.execListener.env()(cleanStr(config.path)))).map { f =>
      val obj = new JSONObject()
      obj.put("content", f)
      obj.toString()
    }
    import spark.implicits._
    reader.options(rewriteConfig(config.config)).json(spark.createDataset[String](items))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new RuntimeException(s"save is not supported in ${shortFormat}")
  }

  override def fullFormat: String = "script"

  override def shortFormat: String = fullFormat

}

