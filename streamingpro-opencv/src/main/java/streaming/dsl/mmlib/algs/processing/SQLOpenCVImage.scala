package streaming.dsl.mmlib.algs.processing

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg
import org.bytedeco.javacpp.opencv_core._
import org.bytedeco.javacpp.opencv_imgproc._
import org.bytedeco.javacpp.opencv_imgcodecs._
import streaming.dsl.mmlib.algs.{MetaConst, SQlBaseFunc, SeqResource}
import streaming.dsl.mmlib.algs.processing.image.{ImageOp, ImageSchema}
import org.apache.spark.ml.linalg.SQLDataTypes._
import streaming.dsl.mmlib.algs.MetaConst._

/**
  * Created by allwefantasy on 28/5/2018.
  */
class SQLOpenCVImage extends SQLAlg with SQlBaseFunc {

  def interval_train(df: DataFrame, path: String, params: Map[String, String]) = {
    val inputCol = params.getOrElse("inputCol", "")
    val metaPath = MetaConst.getMetaPath(path)
    val Array(width, height, channel) = params("shape").split(",").map(f => f.toInt)
    saveTraningParams(df.sparkSession, params, metaPath)
    val spark = df.sparkSession
    val decodeImage = (a: Array[Byte]) => {
      ImageSchema.decode("", a).getOrElse(ImageSchema.invalidImageRow(""))
    }
    val imageRdd = df.rdd.map { f =>
      val index = f.schema.fieldNames.indexOf(inputCol)
      val image = if (f.schema(index).dataType.getClass.getSimpleName == "StructType") {
        f.getStruct(index)
      } else {
        decodeImage(f.getAs[Array[Byte]](inputCol)).getStruct(0)
      }

      var cvImage: IplImage = null
      var targetImage: IplImage = null
      var data: Array[Byte] = Array()
      try {
        cvImage = ImageOp.create(image)
        targetImage = ImageOp.createHeader(width, height, ImageSchema.getDepth(image), ImageSchema.getNChannels(image))
        cvResize(cvImage, targetImage)
        data = ImageOp.getData(targetImage)
      }
      finally {
        // release resource
        if (cvImage != null)
          cvImage.close()
        if (targetImage != null)
          targetImage.close()
      }

      Row(Row(ImageSchema.getOrigin(image), height.toInt, width.toInt,
        channel.toInt,
        ImageSchema.getMode(image),
        data
      ))
    }.filter(f => ImageSchema.getData(f.getStruct(0)).length > 0)

    val newDF = spark.createDataFrame(imageRdd, StructType(df.schema.filter(f => f.name != inputCol) ++ Seq(StructField(name = inputCol, ImageSchema.columnSchema))))
    newDF
  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val newDF = interval_train(df, path, params)
    newDF.write.mode(SaveMode.Overwrite).parquet(MetaConst.getDataPath(path))
  }

  override def load(spark: SparkSession, _path: String, params: Map[String, String]): Any = {
    val path = getMetaPath(_path)
    import spark.implicits._
    val df = spark.read.parquet(PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
    val trainParams = df.collect().toMap
    trainParams
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {

    val trainParams = _model.asInstanceOf[Map[String, String]]
    val Array(width, height, channel) = trainParams("shape").split(",").map(f => f.toInt)
    val decodeImage = (a: Array[Byte]) => {
      ImageSchema.decode("", a).getOrElse(ImageSchema.invalidImageRow(""))
    }
    val func = (a: Array[Byte]) => {
      val image = decodeImage(a).getStruct(0)

      var cvImage: IplImage = null
      var targetImage: IplImage = null
      var data: Array[Byte] = Array()
      try {
        cvImage = ImageOp.create(image)
        targetImage = ImageOp.createHeader(width, height, ImageSchema.getDepth(image), ImageSchema.getNChannels(image))
        cvResize(cvImage, targetImage)
        data = ImageOp.getData(targetImage)
      }
      finally {
        // release resource
        if (cvImage != null)
          cvImage.close()
        if (targetImage != null)
          targetImage.close()
      }
      Row(ImageSchema.getOrigin(image), height.toInt, width.toInt,
        ImageSchema.getNChannels(image),
        ImageSchema.getMode(image),
        data
      )
    }
    UserDefinedFunction(func, ImageSchema.columnSchema, Some(Seq(BinaryType)))
  }
}



