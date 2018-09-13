package streaming.dsl.mmlib.algs

import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 14/1/2018.
  */
class SQLKMeans extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val rfc = new BisectingKMeans()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = BisectingKMeansModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[BisectingKMeansModel])
    val f = (v: Vector) => {
      model.value.getClass.getDeclaredMethod("predict", classOf[Vector]).invoke(model.value, v).asInstanceOf[Int]
    }
    UserDefinedFunction(f, IntegerType, Some(Seq(VectorType)))
  }
}
