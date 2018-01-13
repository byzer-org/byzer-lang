package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLWord2Vec extends SQLAlg {

  def train(df: DataFrame, path: String, params: Map[String, String]) = {
    val w2v = new Word2Vec()
    w2v.setInputCol(params("inputCol"))
    w2v.setMinCount(0)
    val model = w2v.fit(df)
    model.write.overwrite().save(path)
  }

  def load(path: String) = {
    val model = Word2VecModel.load(path)
    model.getVectors.collect().
      map(f => (f.getAs[String]("word"), f.getAs[DenseVector]("vector").toArray)).
      toMap
  }

  def predict(_model: Any): UserDefinedFunction = {
    val model = _model.asInstanceOf[Map[String, Array[Double]]]

    val f = (co: String) => {
      model.get(co) match {
        case Some(vec) => vec
        case None => null
      }

    }
    UserDefinedFunction(f, ArrayType(DoubleType), Some(Seq(StringType)))
  }
}
