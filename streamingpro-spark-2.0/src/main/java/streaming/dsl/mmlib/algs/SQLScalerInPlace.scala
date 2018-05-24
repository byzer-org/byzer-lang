package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import MetaConst._
import org.apache.spark.ml.linalg.Vectors
import streaming.dsl.mmlib.algs.feature.{DoubleFeature, StringFeature}
import streaming.dsl.mmlib.algs.meta.ScaleMeta
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.types.{ArrayType, DoubleType}

/**
  * Created by allwefantasy on 24/5/2018.
  */
class SQLScalerInPlace extends SQLAlg with Functions {

  def internal_train(df: DataFrame, path: String, params: Map[String, String]) = {
    val path = params("path")
    val metaPath = getMetaPath(path)
    saveTraningParams(df.sparkSession, params, metaPath)
    val inputCols = params.getOrElse("inputCols", "").split(",")
    val scaleMethod = params.getOrElse("scaleMethod", "log2")
    val removeOutlierValue = params.getOrElse("removeOutlierValue", "false").toBoolean
    require(!inputCols.isEmpty, "inputCols is required when use SQLScalerInPlace")
    var newDF = df
    if (removeOutlierValue) {
      newDF = DoubleFeature.killOutlierValue(df, metaPath, inputCols)
    }
    newDF = DoubleFeature.scale(df, metaPath, inputCols, scaleMethod, params)
    newDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val newDF = internal_train(df, path, params)
    newDF.write.mode(SaveMode.Overwrite).parquet(getDataPath(path))
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    //load train params
    val path = getMetaPath(_path)
    val (trainParams, df) = getTranningParams(spark, path)
    val inputCols = params.getOrElse("inputCols", "").split(",").toSeq
    val scaleMethod = params.getOrElse("scaleMethod", "log2")
    val removeOutlierValue = params.getOrElse("removeOutlierValue", "false").toBoolean

    val scaleFunc = scaleMethod match {
      case "min-max" =>
        DoubleFeature.getMinMaxModelForPredict(spark, inputCols, path, trainParams)
      case "log2" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log(a))

      case "logn" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log1p(a))

      case "log10" =>
        DoubleFeature.baseRescaleFunc((a) => Math.log10(a))
      case "sqrt" =>
        DoubleFeature.baseRescaleFunc((a) => Math.sqrt(a))

      case "abs" =>
        DoubleFeature.baseRescaleFunc((a) => Math.abs(a))
      case _ =>
        DoubleFeature.baseRescaleFunc((a) => Math.log(a))
    }

    var meta = ScaleMeta(trainParams, null, scaleFunc)

    if (removeOutlierValue) {
      val removeOutlierValueFunc = DoubleFeature.getModelOutlierValueForPredict(spark, path, inputCols, trainParams)
      meta = meta.copy(removeOutlierValueFunc = removeOutlierValueFunc)
    }
    meta
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val meta = _model.asInstanceOf[ScaleMeta]
    val removeOutlierValue = params.getOrElse("removeOutlierValue", "false").toBoolean
    val inputCols = params.getOrElse("inputCols", "").split(",").toSeq
    val f = (values: Seq[Double]) => {
      val newValues = if (removeOutlierValue) {
        values.zipWithIndex.map { v =>
          meta.removeOutlierValueFunc(v._1, inputCols(v._2))
        }
      } else values
      meta.scaleFunc(Vectors.dense(newValues.toArray))
    }
    UserDefinedFunction(f, VectorType, Some(Seq(ArrayType(DoubleType))))
  }
}
