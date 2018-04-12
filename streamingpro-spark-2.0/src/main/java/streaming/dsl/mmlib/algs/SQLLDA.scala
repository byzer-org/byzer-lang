package streaming.dsl.mmlib.algs

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.{LDA, LocalLDAModel}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.clustering.{DistributedLDAModel => OldDistributedLDAModel, EMLDAOptimizer => OldEMLDAOptimizer, LDA => OldLDA, LDAModel => OldLDAModel, LDAOptimizer => OldLDAOptimizer, LocalLDAModel => OldLocalLDAModel, OnlineLDAOptimizer => OldOnlineLDAOptimizer}

import scala.collection.mutable

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLLDA extends SQLAlg with Functions {

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val rfc = new LDA()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    val model = LocalLDAModel.load(path)
    model
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    val model = sparkSession.sparkContext.broadcast(_model.asInstanceOf[LocalLDAModel])
    val field = model.value.getClass.getDeclaredField("oldLocalModel")
    field.setAccessible(true)
    val oldLocalModel = field.get(model.value).asInstanceOf[OldLocalLDAModel]
    val transformer = oldLocalModel.getClass.getMethod("getTopicDistributionMethod", classOf[SparkContext]).
      invoke(oldLocalModel, sparkSession.sparkContext).
      asInstanceOf[(org.apache.spark.mllib.linalg.Vector) => org.apache.spark.mllib.linalg.Vector]


    sparkSession.udf.register(name + "_doc", (v: Vector) => {
      transformer(OldVectors.fromML(v)).asML
    })

    sparkSession.udf.register(name + "_topic", (topic: Int, termSize: Int) => {
      model.value.describeTopics(termSize).where(s"topic=${topic}").collect().map { f =>
        val termIndices = f.getAs[mutable.WrappedArray[Int]]("termIndices")
        val termWeights = f.getAs[mutable.WrappedArray[Double]]("termWeights")
        termIndices.zip(termWeights).toArray
      }.head
    })

    val f = (word: Int) => {
      model.value.topicsMatrix.rowIter.toList(word)
    }
    UserDefinedFunction(f, VectorType, Some(Seq(IntegerType)))
  }
}
