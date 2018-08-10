package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.feature.StringFeature

/**
 * Created by zhuml on 9/8/2018.
 */
class SQLRawSimilarInPlace extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val spark = df.sparkSession

    val inputCol = params.getOrElse("inputCol", "content").toString
    val labelCol = params.getOrElse("labelCol", "label").toString
    val threshold = params.getOrElse("threshold", "0.8").toDouble
    val sentenceSplit = params.getOrElse("sentenceSplit", "。").toString
    val modelType = params.getOrElse("modelType", "Word2VecInPlace").toString
    val modelPath = params("modelPath")
    val newDf = modelType match {
      case _ => StringFeature.raw2vec(df, inputCol, sentenceSplit, modelPath)
    }
    val rdd = newDf.rdd.map(f => (f.getAs(inputCol).asInstanceOf[Seq[Seq[Double]]], f.getAs(labelCol).asInstanceOf[Long]))
    val rdd1 = rdd.cartesian(rdd).filter(x => x._1._2 > x._2._2).map(x => Row(x._1._2, x._2._2, StringFeature.rawSimilar(x._1._1, x._2._1, threshold)))
    val newDf1 = df.sparkSession.createDataFrame(rdd1,
      StructType(Seq(StructField("i", LongType), StructField("j", LongType), StructField("v", DoubleType))))
    newDf1.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(spark: SparkSession, path: String, params: Map[String, String]): Any = {
    val entries = spark.read.parquet(path)
    val rdd1 = entries.rdd.map { f =>
      (f.getLong(0), (f.getLong(1), f.getDouble(2)))
    }
    val rdd2 = entries.rdd.map { f =>
      (f.getLong(1), (f.getLong(0), f.getDouble(2)))
    }
    rdd1.union(rdd2).groupByKey().map(f => (f._1, f._2.toMap)).collect().toMap
  }

  override def predict(spark: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = spark.sparkContext.broadcast(_model.asInstanceOf[Map[Long, Map[Long, Double]]])

    val f = (i: Long, threshhold: Double) => {
      model.value(i).filter(f => f._2 > threshhold)
    }
    UserDefinedFunction(f, MapType(LongType, DoubleType), Some(Seq(LongType, DoubleType)))
  }

}