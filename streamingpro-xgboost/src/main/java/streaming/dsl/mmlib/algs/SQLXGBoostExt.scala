package streaming.dsl.mmlib.algs

import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.SQLAlg
import streaming.log.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 12/9/2018.
  */
class SQLXGBoostExt extends SQLAlg with Logging {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {


    trainModels[XGBoostClassificationModel](df, path, params, () => {
      new XGBoostClassifier()
    })

  }

  override def load(sparkSession: _root_.org.apache.spark.sql.SparkSession, path: String, params: Map[String, String]): Any = {
    loadModels(path, (tempPath) => {
      XGBoostClassificationModel.load(tempPath)
    })
  }

  override def predict(sparkSession: _root_.org.apache.spark.sql.SparkSession, _model: Any, name: String, params: Map[String, String]): _root_.org.apache.spark.sql.expressions.UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  def predict_classification(sparkSession: SparkSession, _model: Any, name: String) = {

    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[Any]])

    val f = (vec: Vector) => {
      models.value.map { model =>
        val predictVector = model.getClass.getMethod("predict", classOf[Vector]).invoke(model, vec).asInstanceOf[Double]
        //概率，分类
        (predictVector, Vectors.dense(Array(predictVector.asInstanceOf[Double])))
      }.sortBy(f => f._1).reverse.head._2
    }

    val f2 = (vec: Vector) => {
      models.value.map { model =>
        val predictVector = model.getClass.getMethod("predict", classOf[Vector]).invoke(model, vec).asInstanceOf[Double]
        Vectors.dense(Array(predictVector.asInstanceOf[Double]))
      }
    }

    sparkSession.udf.register(name + "_raw", f2)

    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }

  def loadModels(path: String, modelType: (String) => Any) = {
    val files = HDFSOperator.listModelDirectory(path).filterNot(_.getPath.getName.startsWith("__"))
    val models = ArrayBuffer[Any]()
    files.foreach { f =>
      val model = modelType(f.getPath.toString)
      models += model
    }
    models
  }

  def owner = {
    try {
      val ssqe = Class.forName("streaming.dsl.ScriptSQLExec")
      //ReflectHelper.method(ssqe, "contextGetOrForTest")
      val context = ssqe.getMethod("contextGetOrForTest").invoke(null)
      context.getClass.getMethod("owner").invoke(context)
    } catch {
      case e: Exception =>
        "testUser"
    }

  }

  def trainModels[T <: Model[T]](df: DataFrame, path: String, params: Map[String, String], modelType: () => Params) = {

    def f(trainData: DataFrame, modelIndex: Int) = {
      val alg = modelType()
      configureModel(alg, params)
      logInfo(s"[owner] [$owner] xgboost ${alg.explainParams()} ")
      //logInfo
      val model = alg.asInstanceOf[Estimator[T]].fit(trainData)
      model.asInstanceOf[MLWritable].write.overwrite().save(path + "/" + modelIndex)
    }
    f(df, 0)
  }

  def configureModel(model: Params, params: Map[String, String]) = {
    model.params.map { f =>
      if (params.contains(f.name)) {
        val v = params(f.name)
        val m = model.getClass.getMethods.filter(m => m.getName == s"set${f.name.capitalize}").head
        val pt = m.getParameterTypes.head
        val v2 = pt match {
          case i if i.isAssignableFrom(classOf[Int]) => v.toInt
          case i if i.isAssignableFrom(classOf[Double]) => v.toDouble
          case i if i.isAssignableFrom(classOf[Float]) => v.toFloat
          case i if i.isAssignableFrom(classOf[Boolean]) => v.toBoolean
          case _ => v
        }
        m.invoke(model, v2.asInstanceOf[AnyRef])
      }
    }
  }
}
