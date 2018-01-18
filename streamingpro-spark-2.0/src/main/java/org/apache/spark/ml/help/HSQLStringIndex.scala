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
        if (model.value.getHandleInvalid == "keep" || model.value.getHandleInvalid == "skip") {
          -1
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label).toInt
        } else if (model.value.getHandleInvalid == "keep" || model.value.getHandleInvalid == "skip") {
          -1
        } else {
          throw new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to keep.")
        }
      }
    }

    val f2 = (labels: Seq[String]) => {
      if (model.value.getHandleInvalid == "keep")
        labels.map(label => f(label)).toArray
      else labels.map(label => f(label)).filterNot(f => f == -1).toArray
    }

    val f_r = (index: Double) => {
      if (model.value.labels.length <= index.toInt || index.toInt < 0) {
        if (model.value.getHandleInvalid == "keep")
          "__unknow__"
        else throw new SparkException(s"Unseen index: $index.  To handle unseen labels, " +
          s"set Param handleInvalid to keep.")
      }
      else
        model.value.labels(index.toInt)
    }

    sparkSession.udf.register(name + "_array", f2)
    sparkSession.udf.register(name + "_r", f_r)
    sparkSession.udf.register(name + "_rarray", (indexs: Seq[Double]) => {
      indexs.map(index => f_r(index)).filterNot(f => f == "__unknow__").toArray
    })
    UserDefinedFunction(f, DoubleType, Some(Seq(StringType)))
  }
}
