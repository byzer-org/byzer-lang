package streaming.test.deeplearning

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.test.image.Minist

/**
  * 2018-11-29 WilliamZhu(allwefantasy@gmail.com)
  */
class BigDLSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "SQLBigDLClassifyExt" should "runs without exception [SQLBigDLClassifyExt]" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val ministPath = Minist.downloadMnist()
      val modelPath = s"/tmp/${UUID.randomUUID().toString}"

      val code =
        s"""
          |set json = '''{}''';
          |load jsonStr.`json` as emptyData;
          |
          |run emptyData as MnistLoaderExt.`` where
          |mnistDir="${ministPath}"
          |as data;
          |
          |set modelOutputPath = "${modelPath}";
          |
          |train data as BigDLClassifyExt.`$${modelOutputPath}` where
          |fitParam.0.featureSize="[28,28]"
          |and fitParam.0.classNum="10"
          |and fitParam.0.maxEpoch="1"
          |and fitParam.0.code='''
          |                   def apply(params:Map[String,String])={
          |                        val model = Sequential()
          |                        model.add(Reshape(Array(1, 28, 28), inputShape = Shape(28, 28, 1)))
          |                        model.add(Convolution2D(6, 5, 5, activation = "tanh").setName("conv1_5x5"))
          |                        model.add(MaxPooling2D())
          |                        model.add(Convolution2D(12, 5, 5, activation = "tanh").setName("conv2_5x5"))
          |                        model.add(MaxPooling2D())
          |                        model.add(Flatten())
          |                        model.add(Dense(100, activation = "tanh").setName("fc1"))
          |                        model.add(Dense(params("classNum").toInt, activation = "softmax").setName("fc2"))
          |                    }
          |
          |'''
          |;
          |predict data as BigDLClassifyExt.`$${modelOutputPath}`;
          |
          |register BigDLClassifyExt.`$${modelOutputPath}` as mnistPredict;
          |
          |select
          |vec_argmax(mnistPredict(vec_dense(features))) as predict_label,
          |label from data
          |as output;
        """.stripMargin
      val sq = createSSEL
      try {
        ScriptSQLExec.parse(code, sq)
      } finally {
        FileUtils.deleteDirectory(new File(modelPath))
      }

    }
  }
}


