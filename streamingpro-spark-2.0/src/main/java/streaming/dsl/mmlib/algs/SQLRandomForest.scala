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
    trainModels[RandomForestClassificationModel](df, path, params, () => {
      new RandomForestClassifier()
    })
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    loadModels(path, (tempPath) => {
      RandomForestClassificationModel.load(tempPath)
    })
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }
}
