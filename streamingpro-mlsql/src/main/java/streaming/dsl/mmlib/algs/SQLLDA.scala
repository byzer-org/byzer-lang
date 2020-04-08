/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import org.apache.spark.ml.clustering.{LDA, LDAModel, LocalLDAModel}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SparkSession}
import streaming.dsl.mmlib.{AlgType, Code, CoreVersion, Core_2_2_x, Core_2_3_x, Core_2_4_x, Doc, HtmlDoc, ModelType, SQLAlg, SQLCode}
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
    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    val rfc = new LDA()
    configureModel(rfc, params)
    rfc.fit(df)

    trainModelsWithMultiParamGroup[LDAModel](df, path, params, () => {
      rfc
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[LDAModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    formatOutput(getModelMetaData(spark, path))
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new LDA()
    })
  }

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[LDAModel]].head
    model.transform(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = LocalLDAModel.load(bestModelPath(0))
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
    MLSQLUtils.createUserDefinedFunction(f, VectorType, Some(Seq(IntegerType)))
  }


  override def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_3_x)
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="http://en.wikipedia.org/wiki/LDA">LDA</a> learning algorithm for
      | classification.
      | It supports both binary and multiclass labels, as well as both continuous and categorical
      | features.
      |
      |1. To load data
      |```
      |load libsvm.`D:/soucecode/spark-2.3-src/data/mllib/sample_lda_libsvm_data.txt` as data1;
      |```
      |
      |2. To train LDA Model
      |```
      |train data1 as LDA.`/tmp/model` where
      |```
      |
      |-- k: number of topics, or number of clustering centers
      |```
      |k="3"
      |```
      |
      |-- docConcentration: the hyperparameter (Dirichlet distribution parameter) of article distribution must be >1.0. The larger the value is, the smoother the predicted distribution is
      |```
      |and docConcentration="3.0"
      |```
      |
      |-- topictemperature: the hyperparameter (Dirichlet distribution parameter) of the theme distribution must be >1.0. The larger the value is, the more smooth the distribution can be inferred
      |```
      |and topicConcentration="3.0"
      |```
      |
      |-- maxIterations: number of iterations, which need to be fully iterated, at least 20 times or more
      |```
      |and maxIter="100"
      |```
      |
      |-- setSeed: random seed
      |```
      |and seed="10"
      |```
      |
      |-- checkpointInterval: interval of checkpoints during iteration calculation
      |```
      |and checkpointInterval="10"
      |```
      |
      |-- optimizer: optimized calculation method currently supports "em" and "online". Em method takes up more memory, and multiple iterations of memory may not be enough to throw a stack exception
      |```
      |and optimizer="online";
      |```
      |
      |3. register LDA to UDF
      |```
      |register LDA.`C:/tmp/model` as lda;
      |```
      |
      |4. use LDA udf
      |```
      |select label,lda(4) topicsMatrix,lda_doc(features) TopicDistribution,lda_topic(label,4) describeTopics from data as result;
      |```
      |
      |5. save result
      |```
      |save overwrite result as json.`/tmp/result`;
      |```
      |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |-- load data
      |load libsvm.`D:/soucecode/spark-2.3-src/data/mllib/sample_lda_libsvm_data.txt` as data1;
      |
      |-- use LDA
      |train data1 as LDA.`/tmp/model` where
      |
      |-- k: number of topics, or number of clustering centers
      |k="3"
      |
      |-- docConcentration: the hyperparameter (Dirichlet distribution parameter) of article distribution must be >1.0. The larger the value is, the smoother the predicted distribution is
      |and docConcentration="3.0"
      |
      |-- topictemperature: the hyperparameter (Dirichlet distribution parameter) of the theme distribution must be >1.0. The larger the value is, the more smooth the distribution can be inferred
      |and topicConcentration="3.0"
      |
      |-- maxIterations: number of iterations, which need to be fully iterated, at least 20 times or more
      |and maxIter="100"
      |
      |-- setSeed: random seed
      |and seed="10"
      |
      |-- checkpointInterval: interval of checkpoints during iteration calculation
      |and checkpointInterval="10"
      |
      |-- optimizer: optimized calculation method currently supports "em" and "online". Em method takes up more memory, and multiple iterations of memory may not be enough to throw a stack exception
      |and optimizer="online"
      |
      |;
      |
      |-- register LDA
      |register LDA.`C:/tmp/model` as lda;
      |
      |select label,lda(4) topicsMatrix,lda_doc(features) TopicDistribution,lda_topic(label,4) describeTopics from data as result;
      |
      |save overwrite result as json.`/tmp/result`;
      |
    """.stripMargin)
}
