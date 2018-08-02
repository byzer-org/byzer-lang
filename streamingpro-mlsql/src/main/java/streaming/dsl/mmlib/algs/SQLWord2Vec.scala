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

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = {
    val model = Word2VecModel.load(path)
    model.getVectors.collect().
      map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
      toMap
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Array[Double]]])

    val f = (co: String) => {
      model.value.get(co) match {
        case Some(vec) => vec.toSeq
        case None => Seq[Double]()
      }

    }

    val f2 = (co: Seq[String]) => {
      co.map(f(_)).filter(x => x.size > 0)
    }

    Map((name + "_array") -> f2, name -> f)
  }

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val res = internal_predict(sparkSession, _model, name)
    sparkSession.udf.register(name + "_array", res(name + "_array"))
    UserDefinedFunction(res(name), ArrayType(DoubleType), Some(Seq(StringType)))
  }
}
