package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 8/2/2018.
  */
class SQLDicTokenMatcher extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    require(params.contains("dic.paths") && params.contains("dic.names"))
    val session = df.sparkSession
    val result = params("dic.names").split(",").zip(params("dic.paths").split(",")).map { f =>
      val wordsList = session.sparkContext.textFile(f._2).collect()
      (f._1, wordsList)
    }.map(f => Row.fromSeq(Seq(f._1, f._2)))
    val model = session.createDataFrame(
      session.sparkContext.parallelize(result),
      StructType(Seq(StructField("name", StringType), StructField("tokens", ArrayType(StringType)))))
    //model is also is a table
    model.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    sparkSession.read.parquet(path).collect().map(f => (f.getString(0), f.getSeq(1))).toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[String, Seq[String]]])

    val f = (words: Seq[String], name: String) => {
      model.value(name).intersect(words)
    }


    val wordFilterWithDictionaryAndIndex = (words: Seq[String], name: String, splitter: String) => {
      val dicWords = model.value(name).toSet
      words.zipWithIndex.filter(f => dicWords.contains(f._1)).
        map(f => s"${f._1}${splitter}${f._2}")

    }

    val wordIndex = (words: Seq[String], word: String, splitter: String) => {
      words.zipWithIndex.filter(f => word == f._1).
        map(f => s"${f._1}${splitter}${f._2}")
    }

    sparkSession.udf.register(name + "_dic", wordFilterWithDictionaryAndIndex)
    sparkSession.udf.register(name + "_word", wordIndex)

    UserDefinedFunction(f, ArrayType(StringType), Some(Seq(ArrayType(StringType), StringType)))
  }
}
