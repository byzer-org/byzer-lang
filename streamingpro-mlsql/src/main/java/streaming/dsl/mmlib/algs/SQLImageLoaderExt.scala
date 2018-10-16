package streaming.dsl.mmlib.algs

import java.util.UUID

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.transform.vision.image._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.common.{ScriptCacheKey, SourceCodeCompiler}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.BaseParams
import streaming.log.{Logging, WowLog}
import streaming.session.MLSQLException


class SQLImageLoaderExt(override val uid: String) extends SQLAlg with BaseParams with Logging with WowLog {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    Engine.init
    params.get(imageDir.name).
      map(m => set(imageDir, m)).getOrElse {
      set(imageDir, path)
      require($(imageDir) != null, "imageDir should not empty")
    }

    params.get(numOfImageTasks.name).map(m => set(numOfImageTasks, m.toInt)).getOrElse {
      set(numOfImageTasks, 2)
    }

    val c = ScriptSQLExec.contextGetOrForTest()
    val trans = params.get(code.name).map(m => set(code, m)) match {
      case Some(_) =>
        val wrap = (fn: () => Any) => {
          try {
            ScriptSQLExec.setContextIfNotPresent(c)
            fn()
          } catch {
            case e: Exception =>
              logError(format_exception(e))
              throw e
          }
        }

        val wrapClass = (function: String) => {
          val className = s"StreamingProUDF_${UUID.randomUUID().toString.replaceAll("-", "")}"
          val newfun =
            s"""
               |class  ${className}{
               |import com.intel.analytics.bigdl.transform.vision.image._
               |import com.intel.analytics.bigdl.transform.vision.image.augmentation._
               |import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
               |  ${function}
               |}
            """.stripMargin
          (className, newfun)
        }
        val (className, newfun) = wrapClass($(code))

        val clazz = wrap(() => {
          SourceCodeCompiler.execute(ScriptCacheKey(newfun, className))
        }).asInstanceOf[Class[_]]

        val method = SourceCodeCompiler.getMethod(clazz, "apply")
        Option(method.invoke(clazz.newInstance(), params).asInstanceOf[FeatureTransformer])

      case None => None
    }

    val distributedImageFrame = ImageFrame.read(path, df.sparkSession.sparkContext, $(numOfImageTasks))
    val imageFrame = trans.map(tr => tr(distributedImageFrame)).getOrElse(distributedImageFrame)

    val imageRDD = imageFrame.toDistributed().rdd.map { im =>
      (im.uri, im[Sample[Float]](ImageFeature.sample).getData())
    }
    val imageDF = df.sparkSession.createDataFrame(imageRDD)
      .withColumnRenamed("_1", "imageName")
      .withColumnRenamed("_2", "features")
    imageDF
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }

  override def modelType: ModelType = ProcessType


  override def doc: Doc = Doc(MarkDownDoc,
    """
      |ImageLoaderExt module is used to process images.
      |
      |Check available params:
      |
      |```sql
      |load modelParams.`ImageLoaderExt` as output;
      |```
      |
      |Check example:
      |
      |```
      |load modelExample.`ImageLoaderExt` as output;
      |```
      |
      |The `code` param is used to configure image processing pipeline.
      |MLSQL provide a DSL which is supported by BigDL.
      |
      |For example, if you define a processing pipeline like this(check example)
      |
      |```
      |code='''
      |        def apply(params:Map[String,String]) = {
      |         Resize(256, 256) -> CenterCrop(224, 224) ->
      |          MatToTensor() -> ImageFrameToSample()
      |       }
      |```
      |
      |The parameter of params in apply function is contains all expressions in where/options statement.
      |
      |```
      |Resize(256, 256) -> CenterCrop(224, 224) ->
      |          MatToTensor() -> ImageFrameToSample()
      |```
      |
      |This means first step, resize the image to 256*256 ,and then crop the image in center ,change the image
      |to tensor, finally convert to ImageFrame which is a collection of sample.
      |
      |ImageFrame is a data collections  contains two columns,they are `imageName` and `features`.
      |The imageName is the url of image.
      |
      |More details about FeatureTransformer something like Resize,CenterCrop please check
      |[URL](https://github.com/intel-analytics/BigDL/blob/master/docs/docs/APIGuide/Transformer.md)
    """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    """
      |set json='''{}''';
      |load jsonStr.`json` as emptyData;
      |
      |set imageDir="/Users/allwefantasy/Downloads/jack";
      |
      |run emptyData as ImageLoaderExt.`${imageDir}`
      |where code='''
      |        def apply(params:Map[String,String]) = {
      |         Resize(256, 256) -> CenterCrop(224, 224) ->
      |          MatToTensor() -> ImageFrameToSample()
      |       }
      |''' as images;
      |select imageName from images limit 1 as output;
    """.stripMargin)

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new MLSQLException(s"register is not support in ${getClass.getName}")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val imageDir: Param[String] = new Param[String](this, "imageDir", "imageDir directory")
  final val numOfImageTasks: IntParam = new IntParam(this, "numOfImageTasks", "how many")
  final val code: Param[String] = new Param[String](this, "code", "code")

}
