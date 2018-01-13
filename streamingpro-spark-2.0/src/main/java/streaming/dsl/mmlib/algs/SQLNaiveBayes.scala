package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
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

  override def load(path: String): Any = {
    val model = NaiveBayesModel.load(path)
    model
  }

  override def predict(_model: Any): UserDefinedFunction = {
    val model = _model.asInstanceOf[NaiveBayesModel]

    val f = (vec: Vector) => {
      val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
      val raw2probability = model.getClass.getMethod("raw2probability", classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
      //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
      raw2probability

    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
}
