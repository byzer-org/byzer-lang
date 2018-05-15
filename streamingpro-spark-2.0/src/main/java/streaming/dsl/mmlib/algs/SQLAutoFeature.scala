package streaming.dsl.mmlib.algs

import java.util.UUID

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.feature.{DiscretizerIntFeature, HighOrdinalDoubleFeature}


/**
  * Created by allwefantasy on 2/5/2018.
  */
class SQLAutoFeature extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val intFields = df.schema.filter(f => f.dataType == IntegerType)
    val doubleFields = df.schema.filter(f => f.dataType == DoubleType)
    val textFields = params.getOrElse("textFields", "").split(",").filterNot(f => f.isEmpty).toSet
    val analyseFields = df.schema.filter(f => f.dataType == StringType).filter(f => textFields.contains(f.name))
    var newDF = df

    val mappingPath = if (params.contains("mappingPath")) params("mappingPath") else ("/tmp/" + UUID.randomUUID().toString)

    val tfidf = new SQLTfIdfInPlace()
    analyseFields.foreach { f =>
      newDF = tfidf.interval_train(df, params + ("inputCol" -> f.name, "mappingPath" -> mappingPath))
    }

    newDF = HighOrdinalDoubleFeature.vectorize(newDF, mappingPath, doubleFields.map(f => f.name))
    newDF = DiscretizerIntFeature.vectorize(newDF, mappingPath, intFields.map(f => f.name))

    val vectorCols = Array("__highOrdinalDoubleFeature__", "_discretizerIntFeature_") ++ analyseFields.map(f => f.name)
    val assembler = new VectorAssembler()
      .setInputCols(vectorCols)
      .setOutputCol("features")

    newDF = assembler.transform(newDF)
    vectorCols.foreach { f =>
      newDF = newDF.drop(f)
    }
    newDF.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}


