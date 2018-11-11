package streaming.test.image

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.mmlib.algs.SQLImageLoaderExt

import scala.collection.mutable

/**
  * Created by allwefantasy on 31/5/2018.
  */
class ImageSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  def getHome = {
    getClass.getResource("").getPath.split("streamingpro\\-mlsql").head
  }

  def getImages() = {
    //sklearn_elasticnet_wine
    getHome + "images"
  }

  "image-process" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      import spark.implicits._
      //
      val loader = new SQLImageLoaderExt()
      val df = Seq.empty[(String, String)].toDF("key", "value")
      val newdf = loader.train(df, getImages, Map(
        "code" ->
          """
            |def apply(params:Map[String,String]) = {
            |         Resize(28, 28) ->
            |          MatToTensor() -> ImageFrameToSample()
            |      }
          """.stripMargin
      ))
      assert(newdf.count() > 1)
      newdf.select(F.col("features")).collect().foreach(row => assert(row(0).asInstanceOf[mutable.WrappedArray[Float]].size == 28 * 28 * 4))

    }
  }

}
