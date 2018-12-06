package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, LinearSVC, LinearSVCModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLLSVM extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val bayes = new LinearSVC()
    configureModel(bayes, params)
    val model = bayes.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = LinearSVCModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[LinearSVCModel])

    val f = (vec: Vector) => {
      val predictRaw = model.value.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model.value, vec).asInstanceOf[Vector]
      val raw2probability = model.value.getClass.getMethod("raw2prediction", classOf[Vector]).invoke(model.value, predictRaw).asInstanceOf[Double]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    UserDefinedFunction(f, DoubleType, Some(Seq(VectorType)))
  }
}
