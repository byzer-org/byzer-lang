package org.apache.spark.sql.delta.sources.mysql.binlog.io

import org.apache.spark.sql.types.{BinaryType, DataType, StructType}

/**
  * 2019-06-14 WilliamZhu(allwefantasy@gmail.com)
  */
class SchemaTool(json: String) {
  val schema = DataType.fromJson(json).asInstanceOf[StructType]

  def getColumnNameByIndex(i: Int) = {
    schema(i).name
  }

  def isBinary(i: Int) = {
    schema(i).dataType == BinaryType
  }
}
