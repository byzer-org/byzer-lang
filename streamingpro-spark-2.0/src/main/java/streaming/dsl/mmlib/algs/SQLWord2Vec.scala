package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLWord2Vec extends SQLAlg with Functions {

  def train(df: DataFrame, path: String, params: Map[String, String]) = {
    val w2v = new Word2Vec()
    configureModel(w2v, params)
    val model = w2v.fit(df)
    model.write.overwrite().save(path)
  }

  def load(sparkSession: SparkSession, path: String) = {
    val model = Word2VecModel.load(path)
    model.getVectors.collect().
      map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
      toMap
  }

  def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Array[Double]]])

    val f = (co: String) => {
      model.value.get(co) match {
        case Some(vec) => vec
        case None => null
      }

    }
    UserDefinedFunction(f, ArrayType(DoubleType), Some(Seq(StringType)))
  }
}
