package streaming.core.datasource.impl

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._

/**
  * 2019-02-25 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLCSV(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "csv"

  override def shortFormat: String = "csv"

  final val header: Param[String] = new Param[String](this, "header", " when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is false.")
  final val delimiter: Param[String] = new Param[String](this, "delimiter", "by default columns are delimited using ,, but delimiter can be set to any character")
  final val quote: Param[String] = new Param[String](this, "quote", "by default the quote character is \", but can be set to any character. Delimiters inside quotes are ignored")
  final val escape: Param[String] = new Param[String](this, "escape", "by default the escape character is \\, but can be set to any character. Escaped quote characters are ignored")
  final val emptyValue: Param[String] = new Param[String](this, "emptyValue", "String representation of an empty value in read and in write")
  final val inferSchema: Param[String] = new Param[String](this, "inferSchema", "automatically infers column types. It requires one extra pass over the data and is false by default")
  final val dateFormat: Param[String] = new Param[String](this, "dateFormat", "specifies a string that indicates the date format to use when reading dates or timestamps. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to both DateType and TimestampType. By default, it is null which means trying to parse times and date by java.sql.Timestamp.valueOf() and java.sql.Date.valueOf().")
  final val codec: Param[String] = new Param[String](this, "codec", "For save; compression codec to use when saving to file. Should be the fully qualified name of a class implementing org.apache.hadoop.io.compress.CompressionCodec or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). Defaults to no compression when a codec is not specified.")
}
