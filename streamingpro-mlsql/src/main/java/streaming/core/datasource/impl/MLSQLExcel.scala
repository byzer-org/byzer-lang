package streaming.core.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._

/**
  * 2019-02-19 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLExcel(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    reader.options(rewriteConfig(config.config)).format(format).load(config.config("_filePath_"))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val format = config.config.getOrElse("implClass", fullFormat)

    writer.options(rewriteConfig(config.config)).format(format).save(config.config("_filePath_"))
  }

  def rewriteConfig(config: Map[String, String]) = {
    config ++ Map("useHeader" -> config.getOrElse("useHeader", "false"))
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(shortFormat, "", "")

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "com.crealytics.spark.excel"

  override def shortFormat: String = "excel"

  final val useHeader: Param[String] = new Param[String](this, "useHeader", "default false")
  final val dataAddress: Param[String] = new Param[String](this, "dataAddress", "Optional, default: \"A1\"; Currently the following address styles are supported:\n\nB3: Start cell of the data. Reading will return all rows below and all columns to the right. Writing will start here and use as many columns and rows as required.\nB3:F35: Cell range of data. Reading will return only rows and columns in the specified range. Writing will start in the first cell (B3 in this example) and use only the specified columns and rows. If there are more rows or columns in the DataFrame to write, they will be truncated. Make sure this is what you want.\n'My Sheet'!B3:F35: Same as above, but with a specific sheet.\nMyTable[#All]: Table of data. Reading will return all rows and columns in this table. Writing will only write within the current range of the table. No growing of the table will be performed. PRs to change this are welcome.")
  final val treatEmptyValuesAsNulls: Param[String] = new Param[String](this, "treatEmptyValuesAsNulls", "Optional, default: true")
  final val inferSchema: Param[String] = new Param[String](this, "inferSchema", "Optional, default: false")
  final val workbookPassword: Param[String] = new Param[String](this, "workbookPassword", "Optional, default None. Requires unlimited strength JCE for older JVMs")
  final val timestampFormat: Param[String] = new Param[String](this, "timestampFormat", "Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]")
  final val sheetName: Param[String] = new Param[String](this, "sheetName", "Optional, For save excel")

}
