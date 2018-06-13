package streaming.dsl.mmlib.algs

import net.csdn.common.logging.Loggers
import org.apache.spark.Partitioner
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import streaming.common.HDFSOperator

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLNaiveBayes extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    trainModels[NaiveBayesModel](df, path, params, () => {
      new NaiveBayes()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    loadModels(path, (tempPath) => {
      NaiveBayesModel.load(tempPath)
    })
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {

    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[NaiveBayesModel]])

    val raw2probabilityMethod = if (sparkSession.version.startsWith("2.3")) "raw2probabilityInPlace" else "raw2probability"

    val f = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        //概率，分类
        (raw2probability(raw2probability.argmax), raw2probability)
      }.sortBy(f => f._1).reverse.head._2
    }

    val f2 = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        raw2probability
      }
    }

    sparkSession.udf.register(name + "_raw", f2)

    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
