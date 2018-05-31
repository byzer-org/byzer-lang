package streaming.core

import org.apache.spark.sql.{Row, SaveMode, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import org.apache.spark.ml.linalg.{Vector, Vectors}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLAutoFeature
import streaming.dsl.mmlib.algs.feature.{DiscretizerIntFeature, DoubleFeature, StringFeature}
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 31/5/2018.
  */
class ImageSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  import streaming.dsl.mmlib.algs.processing.SQLOpenCVImage
  import streaming.dsl.mmlib.algs.processing.image.ImageOp

  "image-process" should "work fine" in {
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
      newDF = new SQLOpenCVImage().interval_train(newDF, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      newDF.collect().foreach { r =>
        val item = r.getStruct(0)
        val iplImage = ImageOp.create(item)
        import org.bytedeco.javacpp.opencv_imgcodecs._
        cvSaveImage("/tmp/abc.png", iplImage)
        iplImage.close()
      }
      val cv = new SQLOpenCVImage()
      val model = cv.load(spark, "/tmp/image", Map())
      val jack = cv.predict(spark, model, "jack", Map())
      spark.udf.register("jack", jack)
      val a = spark.sql("select * from wow").toJSON.collect()
      val b = spark.sql("select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus").toJSON.collect()
      assume(a.head == b.head)

      spark.sql("select vec_image(jack(crawler_request_image(imagePath))) as image from orginal_text_corpus").show(false)
    }
  }

  "image-read-path" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse("load image.`/Users/allwefantasy/CSDNWorkSpace/streamingpro/images` as images;", sq)
      val df = spark.sql("select * from images");
      val newDF = new SQLOpenCVImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      spark.sql("select image.origin from wow").show(false)

    }
  }

  "image-without-decode-read-path" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse("""load image.`/Users/allwefantasy/CSDNWorkSpace/streamingpro/images`  as images ;""", sq)
      var df = spark.sql("select * from images");
      var newDF = new SQLOpenCVImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      spark.sql("select image.origin from wow").show(false)


      ScriptSQLExec.parse("""load image.`/Users/allwefantasy/CSDNWorkSpace/streamingpro/images` options enableDecode="false" as images ;""", sq)
      df = spark.sql("select * from images");
      newDF = new SQLOpenCVImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      spark.sql("select image.* from wow").show(false)

    }
  }
}
