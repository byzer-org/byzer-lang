package streaming.dsl.mmlib.algs

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.help.HSQLStringIndex
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql.{functions => F}

/**
  * Created by allwefantasy on 15/1/2018.
  */
class SQLStringIndex extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    require(params.contains("inputCol"), "inputCol is required")
    val inputCol = params("inputCol")
    var newDf = df
    df.schema.filter(f => f.name == inputCol).head.dataType match {
      case ArrayType(StringType, _) =>
        newDf = df.select(F.explode(F.col(inputCol)).as(inputCol))
      case StringType => // do nothing
      case _ => throw new IllegalArgumentException(s"${params("inputCol")} should be arraytype or stringtype")
    }
    val rfc = new StringIndexer()
    configureModel(rfc, params)
    val model = rfc.fit(newDf)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
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
