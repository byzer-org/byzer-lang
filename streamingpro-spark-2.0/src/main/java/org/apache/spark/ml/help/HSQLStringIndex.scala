package org.apache.spark.ml.help

import org.apache.spark.SparkException
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.util.collection.OpenHashMap

/**
  * Created by allwefantasy on 15/1/2018.
  */
object HSQLStringIndex {
  def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {

    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[StringIndexerModel])

    val f = (label: String) => {

      val labelToIndexField = model.value.getClass.getDeclaredField("org$apache$spark$ml$feature$StringIndexerModel$$labelToIndex")
      labelToIndexField.setAccessible(true)
      val labelToIndex = labelToIndexField.get(model.value).asInstanceOf[OpenHashMap[String, Double]]

      if (label == null) {
        if (model.value.getHandleInvalid == "keep") {
          model.value.labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (model.value.getHandleInvalid == "keep") {
          model.value.labels.length
        } else {
          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to keep.")
        }
      }
    }

    sparkSession.udf.register(name + "_r", (index: Double) => {
      model.value.labels(index.toInt)
    })

    UserDefinedFunction(f, DoubleType, Some(Seq(StringType)))
  }
}
