package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLNaiveBayes extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val bayes = new NaiveBayes()
    configureModel(bayes, params)
    val model = bayes.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    val model = NaiveBayesModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any,name:String): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[NaiveBayesModel])

    val f = (vec: Vector) => {
      val predictRaw = model.value.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model.value, vec).asInstanceOf[Vector]
      val raw2probability = model.value.getClass.getMethod("raw2probability", classOf[Vector]).invoke(model.value, predictRaw).asInstanceOf[Vector]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
