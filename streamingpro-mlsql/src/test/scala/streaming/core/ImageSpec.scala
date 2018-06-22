package streaming.core

import java.io.File

import net.sf.json.JSONObject
import org.apache.spark.sql.{Row, SaveMode, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import org.apache.spark.ml.linalg.{Vector, Vectors}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.SQLAutoFeature
import streaming.dsl.mmlib.algs.feature.{DiscretizerIntFeature, DoubleFeature, StringFeature}
import streaming.dsl.mmlib.algs.processing.image.ImageSchema
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 31/5/2018.
  */
class ImageSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  import streaming.dsl.mmlib.algs.processing.SQLOpenCVImage
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

  "image-read-path" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse("load image.`/Users/allwefantasy/CSDNWorkSpace/streamingpro/images` as images;", sq)
      val df = spark.sql("select * from images");
      val newDF = new SQLOpenCVImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      newDF.createOrReplaceTempView("wow")
      spark.sql("select image.* from wow").show(false)

    }
  }

  "image-read-path-without-resize" should "work fine" taggedAs (NotToRunTag) in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL
      ScriptSQLExec.parse("load image.`/Users/allwefantasy/CSDNWorkSpace/streamingpro/images` as images;", sq)
      spark.sql("select image.origin from images").show(false)
      val f = spark.sql("select image from images where image.origin='file:/tmp/william/Users/allwefantasy/CSDNWorkSpace/streamingpro/images/Snip20160510_1.png'")
        .collect().head
      println(ImageSchema.toSeq(f.getStruct(0)).take(100))
      ImageOp.saveImage("/tmp/abc.png", ImageOp.create(f.getStruct(0)))

      val sz = ImageOp.readImage("/tmp/william/Users/allwefantasy/CSDNWorkSpace/streamingpro/images/Snip20160510_1.png")
      println(ImageSchema.toSeq(Row("", "", "", "", "", sz)).take(100))
      val image = f.getStruct(0)

      val ipimage2 = ImageOp.createHeader(ImageSchema.getWidth(image), ImageSchema.getHeight(image), ImageSchema.getDepth(image), ImageSchema.getNChannels(image))
      ipimage2.imageData().put(sz: _*)
      val wow = Array.ofDim[Byte](ImageSchema.getWidth(image) * ImageSchema.getHeight(image) * ImageSchema.getNChannels(image))
      ipimage2.imageData().get(wow)
      println(ImageSchema.toSeq(Row("", "", "", "", "", wow)).take(100))

      val image2 = ImageOp.create(f.getStruct(0))
      val targetImage = ImageOp.createHeader(300, 300, ImageSchema.getDepth(image), ImageSchema.getNChannels(image))
      ImageOp.resize(image2, targetImage)

      ImageOp.saveImage("/tmp/bbc.png", targetImage)

      val df = spark.sql("select image from images where image.origin='file:/tmp/william/Users/allwefantasy/CSDNWorkSpace/streamingpro/images/Snip20160510_1.png'")
      val newDF = new SQLOpenCVImage().interval_train(df, "/tmp/image", Map("inputCol" -> "image", "shape" -> "100,100,4"))
      val image3 = newDF.collect().head.getStruct(0)
      val ipimage3 = ImageOp.create(image3)
      //ImageOp.saveImage("/tmp/bbb.png",ipimage3)
      println(ImageSchema.toSeq(Row("", "", "", "", "", ImageOp.getData(ipimage3))).take(100))

      import scala.collection.JavaConversions._
      val path = "/Users/allwefantasy/Downloads/part-00006-e83c7d56-30e6-4753-b1d0-3b6a7a37d3e5-c000.json"
      scala.io.Source.fromFile(new File(path)).getLines().foreach { line =>
        val obj = JSONObject.fromObject(line)
        val path = obj.getString("origin")
        val bytes = obj.getJSONArray("UDF:vec_array(UDF:vec_image(image))").map(f => f.asInstanceOf[Double].toInt.toByte).toArray
        val ipimage = ImageOp.create(Row(path, 100, 100, 3, "CV_8UC3", bytes))
        ImageOp.saveImage("/tmp/images/" + path.split("/").last, ipimage)
      }

    }
  }

  "image-without-decode-read-path" should "work fine" taggedAs (NotToRunTag) in {
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
