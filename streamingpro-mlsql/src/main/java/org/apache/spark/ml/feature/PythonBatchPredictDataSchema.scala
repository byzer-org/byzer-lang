package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.MatrixUDT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

/**
 * Created by fchen on 2018/8/26.
 */
object PythonBatchPredictDataSchema {
  /**
   * 数据结构, size = batchSize
   * {
   *   originalDatas: indexSeq
   *   combineFeatures: Matrix
   * }
   */
  val newSchema: (DataFrame) => StructType = df => {
    StructType(Seq(
      StructField("originalData", ArrayType(df.schema)),
      StructField("newFeature", new MatrixUDT())
    ))
  }
}
