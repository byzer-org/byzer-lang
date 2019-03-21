package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLCsvStr(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val items = cleanBlockStr(context.execListener.env()(cleanStr(config.path))).split("\n")
    val spark = config.df.get.sparkSession
    import spark.implicits._
    reader.options(rewriteConfig(config.config)).csv(spark.createDataset[String](items))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new RuntimeException(s"save is not supported in ${shortFormat}")
  }

  override def fullFormat: String = "csvStr"

  override def shortFormat: String = fullFormat

}

