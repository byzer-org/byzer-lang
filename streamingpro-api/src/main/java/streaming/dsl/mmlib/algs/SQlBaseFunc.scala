package streaming.dsl.mmlib.algs

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by allwefantasy on 28/5/2018.
  */
trait SQlBaseFunc {
  def saveTraningParams(spark: SparkSession, params: Map[String, String], metaPath: String) = {
    // keep params
    spark.createDataFrame(
      spark.sparkContext.parallelize(params.toSeq).map(f => Row.fromSeq(Seq(f._1, f._2))),
      StructType(Seq(
        StructField("key", StringType),
        StructField("value", StringType)
      ))).write.
      mode(SaveMode.Overwrite).
      parquet(MetaConst.PARAMS_PATH(metaPath, "params"))
  }

  def getTranningParams(spark: SparkSession, metaPath: String) = {
    import spark.implicits._
    val df = spark.read.parquet(MetaConst.PARAMS_PATH(metaPath, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    (trainParams, df)
  }

  def cleanly[A, B](resource: => A)(cleanup: A => Unit)(code: A => B): Option[B] = {
    try {
      val r = resource
      try {
        Some(code(r))
      }
      finally {
        cleanup(r)
      }
    } catch {
      case e: Exception => None
    }
  }
}

class SeqResource[T](resources: Seq[T], cleanup: T => Unit) {
  def close: Unit = {
    resources.foreach(f => cleanup(f))
  }
}
