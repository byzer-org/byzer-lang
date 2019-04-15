package streaming.core.datasource.impl

import org.apache.spark.ml.param.{DoubleParam, Param}
import streaming.core.datasource.MLSQLBaseFileSource
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}

/**
  * 2019-02-19 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLXML(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "com.databricks.spark.xml"

  override def shortFormat: String = "xml"

  final val rowTag: Param[String] = new Param[String](this, "rowTag", "The row tag of your xml files to treat as a row. For example, in this xml <books> <book><book> ...</books>, the appropriate value would be book. Default is ROW. At the moment, rows containing self closing xml tags are not supported.")
  final val charset: Param[String] = new Param[String](this, "charset", "Defaults to 'UTF-8' but can be set to other valid charset names")
  final val ignoreSurroundingSpaces: Param[String] = new Param[String](this, "ignoreSurroundingSpaces", "Sampling ratio for inferring schema (0.0 ~ 1). Default is 1.")
  final val samplingRatio: DoubleParam = new DoubleParam(this, "samplingRatio", "Default is false. Defines whether or not surrounding whitespaces from values being read should be skipped. ")
  final val mode: Param[String] = new Param[String](this, "mode",
    s"""
       |Default is PERMISSIVE.
       |The mode for dealing with corrupt records during parsing.
       |PERMISSIVE :
       |When it encounters a corrupted record, it sets all fields to null and puts the malformed string into a new field configured by columnNameOfCorruptRecord.
       |When it encounters a field of the wrong datatype, it sets the offending field to null.
       |DROPMALFORMED : ignores the whole corrupted records.
       |FAILFAST : throws an exception when it meets corrupted records.
     """.stripMargin)
  final val attributePrefix: Param[String] = new Param[String](this, "attributePrefix", "The prefix for attributes so that we can differentiate attributes and elements. This will be the prefix for field names. Default is _.")
  final val valueTag: Param[String] = new Param[String](this, "valueTag", "The tag used for the value when there are attributes in the element having no child. Default is _VALUE.")

  final val rootTag: Param[String] = new Param[String](this, "rootTag", "For write mode; Default is ROWS. the root tag of your xml files to treat as the root. For example, in this xml <books> <book><book> ...</books>, the appropriate value would be books.")
  final val compression: Param[String] = new Param[String](this, "compression", "For write mode; compression codec to use when saving to file. Should be the fully qualified name of a class implementing org.apache.hadoop.io.compress.CompressionCodec or one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy). Defaults to no compression when a codec is not specified.")
}
