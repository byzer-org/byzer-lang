package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLGBTRegressor extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rfc = new GBTRegressor()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = GBTRegressionModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[GBTRegressionModel])

    val f = (vec: Vector) => {
      model.value.getClass.getMethod("predict", classOf[Vector]).invoke(model.value, vec)
    }
    UserDefinedFunction(f, DoubleType, Some(Seq(VectorType)))
  }
}
