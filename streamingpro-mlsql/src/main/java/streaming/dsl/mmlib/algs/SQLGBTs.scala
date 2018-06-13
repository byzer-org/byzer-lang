package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLGBTs extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val bayes = new GBTClassifier()
    configureModel(bayes, params)
    val model = bayes.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = GBTClassificationModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[GBTClassificationModel])

    val f = (vec: Vector) => {
      val predictRaw = model.value.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model.value, vec).asInstanceOf[Vector]
      val raw2probability = model.value.getClass.getMethod("raw2probability", classOf[Vector]).invoke(model.value, predictRaw).asInstanceOf[Vector]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
