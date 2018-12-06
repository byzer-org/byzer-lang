package streaming.dsl.load.batch

import org.apache.spark.MLSQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLConfExplain(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = false

  override def explain: DataFrame = {
    val items = MLSQLConf.entries.map(f => Row.fromSeq(Seq(f._2.key, f._2.defaultValueString, f._2.doc))).toSeq
    val rows = sparkSession.sparkContext.parallelize(items, 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = StringType),
        StructField(name = "doc", dataType = StringType)
      )))
  }
}
