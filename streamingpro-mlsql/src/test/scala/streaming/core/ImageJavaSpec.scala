package streaming.core

import java.io.File

import javax.imageio.ImageIO
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec

/**
 * Created by zhuml on 4/6/2018.
 */
class ImageJavaSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  import streaming.dsl.mmlib.algs.processing.SQLJavaImage
  import streaming.dsl.mmlib.algs.processing.image.ImageOp

  "image-process" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //
      val dataRDD = spark.sparkContext.parallelize(Seq(
        Seq("https://tpc.googlesyndication.com/simgad/10310202961328364833"))).map { f =>
        Row.fromSeq(f)
      }

      val df = spark.createDataFrame(dataRDD,
        StructType(Seq(
          StructField("imagePath", StringType)
        )))
      df.createOrReplaceTempView("orginal_text_corpus")
      var newDF = spark.sql("select crawler_request_image(imagePath) as image from orginal_text_corpus")
      newDF = new SQLJavaImage().interval_train(newDF, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      newDF.collect().foreach { r =>
        val image = ImageOp.byte2image(r.getStruct(0)(5).asInstanceOf[Array[Byte]])
        ImageIO.write(image, ImageOp.imageFormat(r.getStruct(0)(5).asInstanceOf[Array[Byte]]), new File("/tmp/image.png"))
      }
      val cv = new SQLJavaImage()
      val model = cv.load(spark, "/tmp/image", Map())
      val jack = cv.predict(spark, model, "jack", Map())
      spark.udf.register("jack", jack)
      val a = spark.sql("select * from wow").toJSON.collect()
      val b = spark.sql("select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus").toJSON.collect()
      assume(a.head == b.head)

      spark.sql("select vec_image(jack(crawler_request_image(imagePath))) as image from orginal_text_corpus").show(false)
    }
  }

  "image-without-decode-read-path" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse("""load image.`/Users/zml/dxy/streamingpro/images` options enableDecode="false" as images ;""", sq)
      val df = spark.sql("select * from images");
      var newDF = new SQLJavaImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      spark.sql("select image.* from wow").show(false)

    }
  }
}
