package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import streaming.dsl.mmlib.SQLAlg

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 24/4/2018.
  */
class SQLTokenExtract extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val session = df.sparkSession
    var result = Array[String]()
    require(params.contains("dic.paths"), "dic.paths is required")
    require(params.contains("inputCol"), "inputCol is required")
    require(params.contains("idCol"), "idCol is required")
    val fieldName = params("inputCol")
    val idCol = params("idCol")

    val parserClassName = params.getOrElse("parser", "org.ansj.splitWord.analysis.NlpAnalysis")
    val forestClassName = params.getOrElse("forest", "org.nlpcn.commons.lang.tire.domain.Forest")
    val deduplicateResult = params.getOrElse("deduplicateResult", "false").toBoolean
    val idStructFiled = df.schema.fields.filter(f => f.name == idCol).head

    result ++= params("dic.paths").split(",").map { f =>
      val wordsList = session.sparkContext.textFile(f).collect()
      wordsList
    }.flatMap(f => f)

    val ber = session.sparkContext.broadcast(result)
    val rdd = df.rdd.mapPartitions { mp =>

      val forest = AnsjFunctions.createForest(forestClassName)
      ber.value.foreach { f =>
        AnsjFunctions.addWord(f, forest)
      }
      mp.map { f =>
        val content = f.getAs[String](fieldName)
        val id = f.get(f.schema.fieldNames.indexOf(idCol))
        val tempWords = AnsjFunctions.extractAllWords(forest, content, deduplicateResult)
        Row.fromSeq(Seq(id, tempWords))
      }
    }
    session.createDataFrame(rdd, StructType(Seq(StructField("id", idStructFiled.dataType), StructField("keywords", ArrayType(StringType))))).
      write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}
