package streaming.dsl.mmlib.algs

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg

/**
  * Created by allwefantasy on 22/1/2018.
  */
class SQLRowMatrix extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rdd = df.rdd.map { f =>
      val v = f.getAs(params.getOrElse("inputCol", "features").toString).asInstanceOf[Vector]
      val label = f.getAs(params.getOrElse("labelCol", "label").toString).asInstanceOf[Long]
      IndexedRow(label, OldVectors.fromML(v))
    }

    val randRowMat = new IndexedRowMatrix(rdd).toBlockMatrix().transpose.toCoordinateMatrix().toRowMatrix()
    var threshhold = 0.0
    if (params.contains("threshhold")) {
      threshhold = params.get("threshhold").map(f => f.toDouble).head
    }
    val csl = randRowMat.columnSimilarities(threshhold)

    val newdf = df.sparkSession.createDataFrame(csl.entries.map(f => Row(f.i, f.j, f.value)),
      StructType(Seq(StructField("i", LongType), StructField("j", LongType), StructField("v", DoubleType))))
    newdf.write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val entries = sparkSession.read.parquet(path)
    entries.rdd.map { f =>
      (f.getLong(0), (f.getLong(1), f.getDouble(2)))
    }.groupByKey().map(f => (f._1, f._2.toMap)).collect().toMap
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[Map[Long, Map[Long, Double]]])

    val f = (i: Long, threshhold: Double) => {
      model.value(i).filter(f => f._2 > threshhold)
    }
    UserDefinedFunction(f, MapType(LongType, DoubleType), Some(Seq(LongType, DoubleType)))
  }
}
