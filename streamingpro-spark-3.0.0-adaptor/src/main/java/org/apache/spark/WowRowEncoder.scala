package org.apache.spark

/**
 * 19/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object WowRowEncoder {
  def toRow(schema: StructType) = {
    RowEncoder.apply(schema).resolveAndBind().createDeserializer()

  }

  def fromRow(schema: StructType) = {
    RowEncoder.apply(schema).resolveAndBind().createSerializer()
  }
}
