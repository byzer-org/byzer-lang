package streaming.source.parser

import org.apache.spark.sql.types.WowStructType
import streaming.parser.SparkTypePaser

/**
  * Created by allwefantasy on 14/9/2018.
  */
case class SourceSchema(_schemaStr: String) {
  private val sparkSchema = SparkTypePaser.cleanSparkSchema(SparkTypePaser.toSparkSchema(_schemaStr).asInstanceOf[WowStructType])

  def schema = sparkSchema

  def schemaStr = _schemaStr
}
