package org.apache.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

/**
 * 19/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object WowRowEncoder {
  def toRow(schema: StructType) = {
    (irow: InternalRow) => {
      RowEncoder.apply(schema).resolveAndBind().fromRow(irow)
    }
  }

  def fromRow(schema: StructType) = {
    (row: Row) => {
      RowEncoder.apply(schema).resolveAndBind().toRow(row)
    }
  }
}
