package streaming.dsl.mmlib.algs

import java.io._

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => F}
import com.salesforce.op.{WowOpWorkflow, _}
import com.salesforce.op.features._
import com.salesforce.op.features.types._
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, render}


/**
  * Created by allwefantasy on 17/9/2018.
  */
class AutoFeature extends Serializable {

  def convert(df: DataFrame, label: String) = {
    var newdf = df

    //convert all int to long
    val toLong = F.udf((a: Int) => {
      a.toLong
    })

    val toDouble = (e: Seq[Expression]) => ScalaUDF(new Function1[Object, Any] with Serializable {
      override def apply(v1: Object): Any = {
        v1.toString.toDouble
      }
    }, DoubleType, e)


    // convert label to double
    val labelCol = newdf.schema.filter(f => f.name == label).head
    newdf = newdf.withColumn(labelCol.name, new Column(toDouble(Seq(F.col(labelCol.name).expr))))

    newdf.schema.filter(f => f.dataType == IntegerType).map { f =>
      newdf = newdf.withColumn(f.name, toLong(F.col(f.name)))
    }
    newdf
  }

  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    implicit val spark = df.sparkSession
    val workflowName = params("workflowName")
    val label = params.getOrElse("labelCol", "label")
    val sanityCheck = params.getOrElse("sanityCheck", "true").toBoolean
    val nonNullable = params.getOrElse("nonNullable", "").split(",").filterNot(_.isEmpty).toSet

    val feature2DataTypeMap = params.getOrElse("featureHint", "").split("\n").filterNot(_.isEmpty).map { f =>
      val Array(fieldName, dataType) = f.split(",").map(k => k.trim)
      val fullDataType = "com.salesforce.op.features.types." + dataType
      (fieldName, fullDataType)
    }.toMap

    val newdf = convert(df, label)

    val (responseFeature, features) = WowFeatureBuilder.fromDataFrame[RealNN](newdf, label, nonNullable, feature2DataTypeMap)
    val autoFeatures = features.transmogrify()
    val finalFeatures = if (sanityCheck) responseFeature.sanityCheck(autoFeatures) else autoFeatures
    val workflow = new WowOpWorkflow()
      .setResultFeatures(responseFeature, finalFeatures).setInputDataset[Row](newdf)
    val fittedWorkflow = workflow.trainFeatureModel()
    fittedWorkflow.save(path + "/model", overwrite = true)
    WorkflowManager.put(workflowName, OpWorkflowInfo(workflowName, workflow))
    //write(workflow)
    val resultDf = fittedWorkflow.computeDataUpTo(finalFeatures)
    resultDf
  }

  def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    implicit val spark = df.sparkSession
    require(params.contains("workflowName"), "workflowName is required")
    require(params.contains("labelCol"), "labelCol is required")
    val workflowName = params("workflowName")
    require(WorkflowManager.get(workflowName) != null, s"workflowName $workflowName is is not exists")
    val label = params("labelCol")
    val workflow = WorkflowManager.get(workflowName).wowOpWorkflow
    val newdf = convert(df, label)
    val fittedWorkflow = workflow.loadModel(path + "/model")
    fittedWorkflow.setInputDataset[Row](newdf).score()
  }

  def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]) = {
    implicit val spark = sparkSession
    val workflowName = params("workflowName")
    require(WorkflowManager.get(workflowName) != null, s"workflowName $workflowName is is not exists")
    val workflow = WorkflowManager.get(workflowName).wowOpWorkflow
    val fittedWorkflow = workflow.loadModel(path + "/model")
    fittedWorkflow.setResultFeatures(workflow.getResultFeatures())

    val feautres = workflow.getResultFeatures().map(f => Row.fromSeq(Seq("feature", compact(render(f.toJson(true))))))
    println(workflow.prettyResultFeaturesDependencyGraphs)
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("resultFeaturesDependencyGraphs", workflow.prettyResultFeaturesDependencyGraphs))
    ) ++ feautres), StructType(Seq(
      StructField(name = "desc", dataType = StringType),
      StructField(name = "command", dataType = StringType)
    )))


  }

  def listWorkflows() = {
    WorkflowManager.items.map(f => f.name)
  }
}


