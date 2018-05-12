package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLStringIndex extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rfc = new StringIndexer()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = StringIndexerModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    HSQLStringIndex.predict(sparkSession, _model, name)
  }

  def internal_predict(sparkSession: SparkSession, _model: Any, name: String) = {
    HSQLStringIndex.internal_predict(sparkSession, _model, name)
  }
}
