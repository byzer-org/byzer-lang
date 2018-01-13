package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rfc = new RandomForestClassifier()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    val model = RandomForestClassificationModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[RandomForestClassificationModel])

    val f = (vec: Vector) => {
      val predictRaw = model.value.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model.value, vec).asInstanceOf[Vector]
      val raw2probability = model.value.getClass.getMethod("raw2probability", classOf[Vector]).invoke(model.value, predictRaw).asInstanceOf[Vector]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
