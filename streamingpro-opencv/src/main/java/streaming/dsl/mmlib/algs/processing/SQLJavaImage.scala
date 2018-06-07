package streaming.dsl.mmlib.algs.processing

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.imgscalr.Scalr
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst._
import streaming.dsl.mmlib.algs.processing.image.{ImageOp, ImageSchema}
import streaming.dsl.mmlib.algs.{MetaConst, SQlBaseFunc}

/**
 * Created by zhuml on 4/6/2018.
 */
class SQLJavaImage extends SQLAlg with SQlBaseFunc {
  def interval_train(df: DataFrame, path: String, params: Map[String, String]) = {
    val inputCol = params.getOrElse("inputCol", "")
    val filterByteSize = params.getOrElse("filterByteSize", "0").toInt
    val method = params.getOrElse("method", "AUTOMATIC")
    val mode = params.getOrElse("mode", "FIT_EXACT")
    val metaPath = MetaConst.getMetaPath(path)
    val Array(width, height, channel) = params("shape").split(",").map(f => f.toInt)
    saveTraningParams(df.sparkSession, params, metaPath)
    val spark = df.sparkSession
    val decodeImage = (origin: String, a: Array[Byte]) => {
      ImageSchema.decode(origin, a).getOrElse(ImageSchema.invalidImageRow(origin))
    }
    val imageRdd = df.rdd.map { f =>
      val index = f.schema.fieldNames.indexOf(inputCol)
      val image = if (f.schema(index).dataType.getClass.getSimpleName == "StructType") {
        val temp = f.getStruct(index)

        ImageSchema.getMode(temp) match {
          case ImageSchema.undecodedImageType => decodeImage(ImageSchema.getOrigin(temp), ImageSchema.getData(temp)).getStruct(0)
          case _ => temp
        }

      } else {
        decodeImage("", f.getAs[Array[Byte]](inputCol)).getStruct(0)
      }
      ImageSchema.getMode(image) match {
        case ImageSchema.undefinedImageType => ImageSchema.invalidImageRow(ImageSchema.getOrigin(image))
        case _ =>
          try {
            val data: Array[Byte] = ImageOp.resize(ImageSchema.getData(image), Scalr.Method.valueOf(method), Scalr.Mode.valueOf(mode), width, height)

            Row(Row(ImageSchema.getOrigin(image), height.toInt, width.toInt,
              ImageSchema.getNChannels(image),
              ImageSchema.getMode(image),
              data
            ))
          } catch {
            case e: Exception =>
              ImageSchema.invalidImageRow(ImageSchema.getOrigin(image))
          }
      }

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
    val method = trainParams.getOrElse("method", "AUTOMATIC")
    val mode = trainParams.getOrElse("mode", "FIT_EXACT")
    val decodeImage = (a: Array[Byte]) => {
      ImageSchema.decode("", a).getOrElse(ImageSchema.invalidImageRow(""))
    }
    val func = (a: Array[Byte]) => {
      val image = decodeImage(a).getStruct(0)
      try {
        val data: Array[Byte] = ImageOp.resize(ImageSchema.getData(image), Scalr.Method.valueOf(method), Scalr.Mode.valueOf(mode), width, height)

        Row(ImageSchema.getOrigin(image), height.toInt, width.toInt,
          ImageSchema.getNChannels(image),
          ImageSchema.getMode(image),
          data
        )
      } catch {
        case e: Exception =>
          Row(ImageSchema.getOrigin(image), -1, -1, -1, ImageSchema.undefinedImageType, Array.ofDim[Byte](0))
      }
    }
    UserDefinedFunction(func, ImageSchema.columnSchema, Some(Seq(BinaryType)))
  }
}



