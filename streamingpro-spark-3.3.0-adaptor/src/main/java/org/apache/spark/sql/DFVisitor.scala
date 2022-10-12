package org.apache.spark.sql

/**
  * 2019-06-03 WilliamZhu(allwefantasy@gmail.com)
  */
object DFVisitor {
  def showString(df: DataFrame, _numRows: Int,
                 truncate: Int = 20,
                 vertical: Boolean = false) = df.showString(_numRows, truncate, vertical)

}
