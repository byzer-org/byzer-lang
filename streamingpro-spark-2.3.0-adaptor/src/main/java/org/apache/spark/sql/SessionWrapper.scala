package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class SessionWrapper(sparkSession: SparkSession) {
  def internalCreateDataFrame(catalystRows: RDD[InternalRow],
                              schema: StructType,
                              isStreaming: Boolean = false) = {
    sparkSession.sqlContext.internalCreateDataFrame(catalystRows, schema, isStreaming = true)
  }
}
