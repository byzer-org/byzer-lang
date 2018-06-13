package org.apache.spark.ml

import org.apache.spark.sql.DataFrame

/**
 * 7/28/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait BaseAlgorithmTransformer {
  def transform(df: DataFrame): DataFrame
}
