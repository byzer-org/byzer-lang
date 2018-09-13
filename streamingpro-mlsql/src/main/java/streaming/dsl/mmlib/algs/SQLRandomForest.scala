package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.DateTime
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest extends SQLAlg with MllibFunctions with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    SQLPythonFunc.incrementVersion(path, keepVersion)

    val evaluateTable = params.get("evaluateTable")
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[RandomForestClassificationModel](df, path, params, () => {
      new RandomForestClassifier()
    }, (_model) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[RandomForestClassificationModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(params.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    val newDF = df.sparkSession.read.parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
    formatOutput(newDF)
  }

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

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }
}
