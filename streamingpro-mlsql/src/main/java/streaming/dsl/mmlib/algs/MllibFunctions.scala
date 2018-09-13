package streaming.dsl.mmlib.algs

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time.DateTime

/**
  * Created by allwefantasy on 25/7/2018.
  */
trait MllibFunctions extends Serializable {

  def formatOutput(newDF: DataFrame) = {
    val schema = newDF.schema
    def formatMetrics(field: StructField, row: Row) = {
      val value = row.getSeq[Row](schema.fieldIndex(field.name))
      value.map(row => s"${row.getString(0)}:  ${row.getDouble(1)}").mkString("\n")
    }
    def formatDate(field: StructField, row: Row) = {
      val value = row.getLong(schema.fieldIndex(field.name))
      new DateTime(value).toString("yyyyMMdd mm:HH:ss:SSS")
    }
    val rows = newDF.collect().flatMap { row =>
      List(Row.fromSeq(Seq("---------------", "------------------"))) ++ schema.fields.map { field =>
        val value = field.name match {
          case "metrics" => formatMetrics(field, row)
          case "startTime" | "endTime" => formatDate(field, row)
          case _ => row.get(schema.fieldIndex(field.name)).toString
        }
        Row.fromSeq(Seq(field.name, value))
      }

    }
    val newSchema = StructType(Seq(StructField("name", StringType), StructField("value", StringType)))
    newDF.sparkSession.createDataFrame(newDF.sparkSession.sparkContext.parallelize(rows, 1), newSchema)
  }


  def multiclassClassificationEvaluate(predictions: DataFrame, congigureEvaluator: (MulticlassClassificationEvaluator) => Unit) = {
    "f1|weightedPrecision|weightedRecall|accuracy".split("\\|").map { metricName =>
      val evaluator = new MulticlassClassificationEvaluator()
        .setMetricName(metricName)
      congigureEvaluator(evaluator)
      MetricValue(metricName, evaluator.evaluate(predictions))
    }.toList
  }

  def mllibModelAndMetaPath(path: String, params: Map[String, String], sparkSession: SparkSession) = {
    val maxVersion = SQLPythonFunc.getModelVersion(path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }
    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    val baseModelPath = if (modelVersion == -1) SQLPythonFunc.getAlgModelPath(path, versionEnabled)
    else SQLPythonFunc.getAlgModelPathWithVersion(path, modelVersion)

    val metaPath = if (modelVersion == -1) SQLPythonFunc.getAlgMetalPath(path, versionEnabled)
    else SQLPythonFunc.getAlgMetalPathWithVersion(path, modelVersion)

    var algIndex = params.getOrElse("algIndex", "-1").toInt
    val autoSelectByMetric = params.getOrElse("autoSelectByMetric", "f1")

    val modelList = sparkSession.read.parquet(metaPath + "/0").collect()
    val bestModelPath = if (algIndex != -1) {
      Seq(baseModelPath + "/" + algIndex)
    } else {
      modelList.map { row =>
        val metric = row(3).asInstanceOf[scala.collection.mutable.WrappedArray[Row]].filter(f => f.getString(0) == autoSelectByMetric).head
        val metricScore = metric.getAs[Double](1)
        (metricScore, row(0).asInstanceOf[String], row(1).asInstanceOf[Int])
      }
        .toSeq
        .sortBy(f => f._1)(Ordering[Double].reverse)
        .take(1)
        .map(f => {
          algIndex = f._3
          baseModelPath + "/" + f._2.split("/").last
        })
    }
    (bestModelPath, baseModelPath, metaPath)
  }

  def saveMllibTrainAndSystemParams(sparkSession: SparkSession, params: Map[String, String], metaPath: String) = {
    val tempRDD = sparkSession.sparkContext.parallelize(Seq(Seq(Map[String, String](), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    sparkSession.createDataFrame(tempRDD, StructType(Seq(
      StructField("systemParam", MapType(StringType, StringType)),
      StructField("trainParams", MapType(StringType, StringType))))).
      write.
      mode(SaveMode.Overwrite).
      parquet(metaPath + "/1")
  }
}

case class MetricValue(name: String, value: Double)
