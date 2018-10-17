package streaming.dsl.mmlib.algs

import org.apache.spark.ml.clustering.{LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.mmlib.{AlgType, Code, CoreVersion, Core_2_2_x, Core_2_3_x, Doc, HtmlDoc, ModelType, SQLAlg, SQLCode}
import org.apache.spark.mllib.clustering.{LocalLDAModel => OldLocalLDAModel}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuqianjin on 11/11/2018.
  */
class SQLLDA(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val rfc = new LDA()
    configureModel(rfc, params)
    val model = rfc.fit(df)
    model.write.overwrite().save(path)
    emptyDataFrame()(df)
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[LDAModel]].head
    model.transform(df)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new LDA()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val model = LocalLDAModel.load(path)
    ArrayBuffer(model)
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val models = load(sparkSession, path, params).asInstanceOf[ArrayBuffer[LocalLDAModel]]
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq(
        Seq("uid", model.uid),
        Seq("vocabSize", model.vocabSize.toString)
      ) ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[LocalLDAModel]])
    val model: LocalLDAModel = models.value.head
    val field = model.getClass.getDeclaredField("oldLocalModel")
    field.setAccessible(true)
    val oldLocalModel = field.get(model).asInstanceOf[OldLocalLDAModel]
    val transformer = oldLocalModel.getClass.getMethod("getTopicDistributionMethod").
      invoke(oldLocalModel).
      asInstanceOf[(org.apache.spark.mllib.linalg.Vector) => org.apache.spark.mllib.linalg.Vector]

    sparkSession.udf.register(name + "_doc", (v: Vector) => {
      transformer(OldVectors.fromML(v)).asML
    })

    sparkSession.udf.register(name + "_topic", (topic: Int, termSize: Int) => {
      models.value.map(model => {
        val result = model.describeTopics(termSize).where(s"topic=${topic}").collect()
        if (result.nonEmpty) {
          result.map { f =>
            val termIndices = f.getAs[mutable.WrappedArray[Int]]("termIndices")
            val termWeights = f.getAs[mutable.WrappedArray[Double]]("termWeights")
            termIndices.zip(termWeights).toArray
          }.head
        } else {
          null
        }
      })
    })

    val f = (word: Int) => {
      model.topicsMatrix.rowIter.toList(word)
    }
    UserDefinedFunction(f, VectorType, Some(Seq(IntegerType)))
  }


  override def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_2_x, Core_2_3_x)
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="http://en.wikipedia.org/wiki/LDA">LDA</a> learning algorithm for
      | classification.
      | It supports both binary and multiclass labels, as well as both continuous and categorical
      | features.
      |
      | Use "load modelParams.`LDA` as output;"
      | 
      |
      | to check the available hyper parameters;
      |
      | Use "load modelExample.`LDA` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |
      | ```
      | load modelExplain.`/tmp/model` where alg="LDA" as outout;
      | ```
      |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use LDA
      |train data1 as LDA.`/tmp/model` where
      |
      |-- register LDA
      |register LDA.`C:/tmp/model` as lda
      |
      |select label,lda(4) topicsMatrix,lda_doc(features) TopicDistribution,lda_topic(label,4) describeTopics from data as result
      |;
    """.stripMargin)
}
