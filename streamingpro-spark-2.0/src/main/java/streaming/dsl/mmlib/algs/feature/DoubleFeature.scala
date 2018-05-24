package streaming.dsl.mmlib.algs.feature

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors, DenseVector => OldDenseVector, SparseVector => OldSparseVector}
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.algs.meta.{MinMaxValueMeta, OutlierValueMeta, StandardScalerValueMeta}

import scala.collection.mutable.ArrayBuffer
import streaming.dsl.mmlib.algs.MetaConst._

/**
  * Created by allwefantasy on 15/5/2018.
  */
object DoubleFeature extends BaseFeatureFunctions {

  def killOutlierValue(df: DataFrame, metaPath: String, fields: Seq[String]): DataFrame = {
    var newDF = df
    val metas = new ArrayBuffer[OutlierValueMeta]()
    fields.foreach { f =>
      val (ndf, meta) = killSingleColumnOutlierValue(newDF, f)
      newDF = ndf
      metas += meta
    }

    val spark = df.sparkSession
    import spark.implicits._
    spark.createDataset(metas).write.mode(SaveMode.Overwrite).
      parquet(OUTLIER_VALUE_PATH(metaPath, getFieldGroupName(fields)))

    newDF
  }

  def getModelOutlierValueForPredict(spark: SparkSession, metaPath: String, fields: Seq[String], trainParams: Map[String, String]) = {
    import spark.implicits._
    val _outlierValueMeta = spark.read.parquet(OUTLIER_VALUE_PATH(metaPath, getFieldGroupName(fields))).as[OutlierValueMeta].collect().
      map(f => (f.fieldName, f)).toMap
    val outlierValueMeta = spark.sparkContext.broadcast(_outlierValueMeta)
    val f = (a: Double, field: String) => {
      val lowerRange = outlierValueMeta.value(field).lowerRange
      val upperRange = outlierValueMeta.value(field).upperRange
      if (a < lowerRange || a > upperRange) {
        outlierValueMeta.value(field).quantile
      } else a
    }
    f
  }

  def vectorize(df: DataFrame, metaPath: String,
                fields: Seq[String],
                outputCol: String = "_features_") = {
    // killOutlierValue
    var newDF = df
    // assemble double fields
    val assembler = new VectorAssembler()
      .setInputCols(fields.toArray)
      .setOutputCol(outputCol)
    newDF = assembler.transform(df)
    newDF
  }

  def getMinMaxModelForPredict(spark: SparkSession, fields: Seq[String], metaPath: String, trainParams: Map[String, String]) = {
    import spark.implicits._
    val metas = spark.read.
      parquet(MIN_MAX_PATH(metaPath, getFieldGroupName(fields))).as[MinMaxValueMeta].collect().map(f => (f.fieldName, f)).toMap

    val minArray = fields.map(f => metas(f).min).toArray
    val maxArray = fields.map(f => metas(f).max).toArray
    val originalMin = Vectors.dense(minArray)
    val originalMax = Vectors.dense(maxArray)

    val originalRange = (asBreeze(originalMax) - asBreeze(originalMin)).toArray
    val min = trainParams.getOrElse("min", "0").toDouble
    val max = trainParams.getOrElse("max", "1").toDouble
    val scaleRange = max - min

    minMaxFunc(originalRange, minArray, scaleRange, min)

  }

  def minMaxFunc(originalRange: Array[Double], minArray: Array[Double], scaleRange: Double, min: Double) = {
    (vector: Vector) => {
      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.length
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scaleRange + min
        }
        i += 1
      }
      Vectors.dense(values)
    }
  }

  def baseRescaleFunc(f: (Double) => Double) = {
    (vector: Vector) => {
      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.length
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          values(i) = f(values(i))
        }
        i += 1
      }
      Vectors.dense(values)
    }
  }

  //log2,log10,ln,abs,sqrt,min-max
  def scale(df: DataFrame, metaPath: String, fields: Seq[String], method: String, params: Map[String, String]) = {
    val tempColName = getTempCol
    var newDF = vectorize(df, metaPath, fields, tempColName)
    val spark = df.sparkSession

    val baseRescale = (vector: Vector, f: (Double) => Double) => {
      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.length
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          values(i) = f(values(i))
        }
        i += 1
      }
      Vectors.dense(values)
    }
    val reScale = method match {
      case "min-max" =>

        val input: RDD[OldVector] = df.select(F.col(tempColName)).rdd.map {
          case Row(v: Vector) => OldVectors.fromML(v)
        }
        val summary = Statistics.colStats(input)
        val (minArray, maxArray) = (summary.min.toArray, summary.max.toArray)

        //save minmax meta
        val metas = minArray.zipWithIndex.map { f =>
          MinMaxValueMeta(fields(f._2), minArray(f._2), maxArray(f._2))
        }
        import spark.implicits._
        spark.createDataset(metas).write.mode(SaveMode.Overwrite).
          parquet(MIN_MAX_PATH(metaPath, getFieldGroupName(fields)))

        //val minArray = fields.map(f => minMaxMeta(f).lowerRange).toArray
        val originalMin = Vectors.dense(minArray)
        val originalMax = Vectors.dense(maxArray)

        val originalRange = (asBreeze(originalMax) - asBreeze(originalMin)).toArray
        val min = params.getOrElse("min", "0").toDouble
        val max = params.getOrElse("max", "1").toDouble
        val scaleRange = max - min

        minMaxFunc(originalRange, minArray, scaleRange, min)

      case "log2" =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.log(a))
        }

      case "logn" =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.log1p(a))
        }

      case "log10" =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.log10(a))
        }
      case "sqrt" =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.sqrt(a))
        }

      case "abs" =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.abs(a))
        }
      case _ =>
        (v: Vector) => {
          baseRescale(v, (a) => Math.log(a))
        }
    }
    val f = UserDefinedFunction(reScale, VectorType, Some(Seq(VectorType)))
    newDF = replaceColumn(newDF, tempColName, f)
    newDF = expandColumnsFromVector(newDF, fields, tempColName)
    newDF
  }

  def getModelNormalizeForPredict(spark: SparkSession, metaPath: String, fields: Seq[String], method: String, trainParams: Map[String, String]) = {
    method match {
      case "standard" =>
        val metas = spark.read.parquet(STANDARD_SCALER_PATH(metaPath, getFieldGroupName(fields)))
          .as[StandardScalerValueMeta].collect().
          map(f => (f.fieldName, f)).toMap
        val mean = Vectors.dense(fields.map(f => metas(f).mean).toArray)
        val std = Vectors.dense(fields.map(f => metas(f).std).toArray)
        lazy val shift: Array[Double] = mean.toArray
        val withStd = true
        val withMean = true
        val transform = (vector: OldVector) => {
          require(mean.size == vector.size)
          if (withMean) {
            // By default, Scala generates Java methods for member variables. So every time when
            // the member variables are accessed, `invokespecial` will be called which is expensive.
            // This can be avoid by having a local reference of `shift`.
            val localShift = shift
            // Must have a copy of the values since it will be modified in place
            val values = vector match {
              // specially handle DenseVector because its toArray does not clone already
              case d: OldDenseVector => d.values.clone()
              case v: Vector => v.toArray
            }
            val size = values.length
            if (withStd) {
              var i = 0
              while (i < size) {
                values(i) = if (std(i) != 0.0) (values(i) - localShift(i)) * (1.0 / std(i)) else 0.0
                i += 1
              }
            } else {
              var i = 0
              while (i < size) {
                values(i) -= localShift(i)
                i += 1
              }
            }
            OldVectors.dense(values)
          } else if (withStd) {
            vector match {
              case OldDenseVector(vs) =>
                val values = vs.clone()
                val size = values.length
                var i = 0
                while (i < size) {
                  values(i) *= (if (std(i) != 0.0) 1.0 / std(i) else 0.0)
                  i += 1
                }
                OldVectors.dense(values)
              case OldSparseVector(size, indices, vs) =>
                // For sparse vector, the `index` array inside sparse vector object will not be changed,
                // so we can re-use it to save memory.
                val values = vs.clone()
                val nnz = values.length
                var i = 0
                while (i < nnz) {
                  values(i) *= (if (std(indices(i)) != 0.0) 1.0 / std(indices(i)) else 0.0)
                  i += 1
                }
                OldVectors.sparse(size, indices, values)
              case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
            }
          } else {
            // Note that it's safe since we always assume that the data in RDD should be immutable.
            vector
          }
        }

        val transformer: Vector => Vector = v => transform(OldVectors.fromML(v)).asML

        transformer

      case _ =>
    }

  }


  //standard,p-norm
  def normalize(df: DataFrame, metaPath: String, fields: Seq[String], method: String, params: Map[String, String]) = {
    val tempColName = getTempCol
    val spark = df.sparkSession
    var newDF = vectorize(df, metaPath, fields, outputCol = tempColName)

    val outputTempColName = getTempCol

    method match {
      case "standard" =>
        val scaler = new StandardScaler()
          .setInputCol(tempColName)
          .setOutputCol(outputTempColName)
          .setWithStd(true)
          .setWithMean(true)

        val scalerModel = scaler.fit(newDF)
        val (meanArray, stdArray) = (scalerModel.mean.toArray, scalerModel.std.toArray)
        //save standardScaler meta
        val metas = meanArray.zipWithIndex.map { f =>
          StandardScalerValueMeta(fields(f._2), meanArray(f._2), stdArray(f._2))
        }
        import spark.implicits._
        spark.createDataset(metas).write.mode(SaveMode.Overwrite).
          parquet(STANDARD_SCALER_PATH(metaPath, getFieldGroupName(fields)))

        newDF = scalerModel.transform(newDF)
        newDF = expandColumnsFromVector(newDF, fields, outputTempColName)
        newDF

      case "p-norm" =>

    }

  }
}
