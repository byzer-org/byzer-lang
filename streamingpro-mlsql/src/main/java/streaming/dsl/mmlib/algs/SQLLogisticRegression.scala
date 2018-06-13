package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.DoubleType
import streaming.dsl.mmlib.SQLAlg

class SQLLogisticRegression extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val lr = new LogisticRegression()
    configureModel(lr, params)

    val model = lr.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = LogisticRegressionModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[LogisticRegressionModel])

    val f = (vec: Vector) => {
      val result = model.value.getClass.getMethod("predict", classOf[Vector]).invoke(model.value, vec)
      result
    }
    UserDefinedFunction(f, DoubleType, Some(Seq(VectorType)))
  }
}
