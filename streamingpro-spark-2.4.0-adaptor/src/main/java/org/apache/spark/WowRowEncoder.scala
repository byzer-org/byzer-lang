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
    val rab = RowEncoder.apply(schema).resolveAndBind()
    (irow: InternalRow) => {
      rab.fromRow(irow)
    }
  }

  def fromRow(schema: StructType) = {
    val rab = RowEncoder.apply(schema).resolveAndBind()
    (row: Row) => {
      rab.toRow(row)
    }
  }
}
