package streaming.dsl.mmlib.algs

import streaming.dl4j.{DL4JModelLoader, DL4JModelPredictor}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork

import streaming.dsl.mmlib.SQLAlg


/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLDL4J extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

    require(params.contains("featureSize"), "featureSize is required")
    require(params.contains("labelSize"), "labelSize is required")
    require(params.contains("alg"), "alg is required")
    Class.forName("streaming.dsl.mmlib.algs.dl4j." + params("alg")).newInstance().asInstanceOf[SQLAlg].train(df, path, params)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    path
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    val f = (v: org.apache.spark.ml.linalg.Vector) => {
      val modelBundle = DL4JModelLoader.load(_model.asInstanceOf[String])
      val res = DL4JModelPredictor.run_double(modelBundle.asInstanceOf[MultiLayerNetwork], v)
      res
    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
