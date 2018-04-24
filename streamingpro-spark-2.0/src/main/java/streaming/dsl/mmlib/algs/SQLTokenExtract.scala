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
    result ++= params("dic.paths").split(",").map { f =>
      val wordsList = session.sparkContext.textFile(f).collect()
      wordsList
    }.flatMap(f => f)
    val ber = session.sparkContext.broadcast(result)
    val rdd = df.rdd.mapPartitions { mp =>

      val forest = Class.forName("org.nlpcn.commons.lang.tire.domain.SmartForest").newInstance()
      ber.value.foreach { f =>
        forest.getClass.getMethod("add", classOf[String], classOf[Object]).invoke(forest, f, new Integer(1))
      }
      mp.map { f =>
        val content = f.getAs[String](fieldName)
        val id = f.getAs[String](idCol)
        //val udg = forest.getWord(q.query.toLowerCase.toCharArray)
        val udg = forest.getClass.getMethod("getWord", classOf[String]).invoke(forest, content.toLowerCase)
        def getAllWords(udg: Any) = {
          udg.getClass.getMethod("getAllWords").invoke(udg).asInstanceOf[String]
        }
        var tempWords = ArrayBuffer[String]()
        var temp = getAllWords(udg)
        while (temp != null) {
          tempWords += temp
          temp = getAllWords(udg)
        }
        Row.fromSeq(Seq(id, tempWords))
      }
    }
    session.createDataFrame(rdd, StructType(Seq(StructField("id", StringType), StructField("keywords", ArrayType(StringType))))).
      write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}
