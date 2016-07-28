package org.apache.spark.sql.execution.datasources.hdfs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

/**
 * 7/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultSource extends RelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified for HDFS data."))
    val fieldName = parameters.getOrElse("fieldName", "raw")
    new HDFSRelation(path, fieldName)(sqlContext)
  }

  override def shortName(): String = "hdfs"
}

private[sql] class HDFSRelation(val path: String,
                                val fieldName: String
                                 )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  override def schema: StructType = StructType(Seq(StructField(fieldName, StringType, false)))

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.textFile(path).map { line =>
      Row.fromSeq(Seq(UTF8String.fromString(line)))
    }
  }
}
