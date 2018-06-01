package streaming.dsl.mmlib.algs

import streaming.tensorflow.TFModelLoader
import streaming.tensorflow.TFModelPredictor
import java.io.{ByteArrayOutputStream, File}
import java.util
import java.util.UUID


import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.util.{ExternalCommandRunner, ObjPickle, WowMD5}
import org.apache.spark.ml.linalg.SQLDataTypes._

import scala.collection.JavaConverters._
import SQLPythonFunc._


/**
  * Created by allwefantasy on 13/1/2018.
  *
  */
class SQLTensorFlow extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val (kafkaParam, newRDD) = writeKafka(df, path, params)
    val systemParam = mapParams("systemParam", params)

    val stopFlagNum = newRDD.getNumPartitions

    val fitParam = arrayParams("fitParam", params).zipWithIndex
    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)

    val userPythonScript = loadUserDefinePythonScript(params, df.sparkSession)

    val schema = df.schema
    var rows = Array[Array[Byte]]()
    //目前我们只支持同一个测试集
    if (params.contains("validateTable")) {
      val validateTable = params("validateTable")
      rows = df.sparkSession.table(validateTable).rdd.mapPartitions { iter =>
        ObjPickle.pickle(iter, schema)
      }.collect()
    }
    val rowsBr = df.sparkSession.sparkContext.broadcast(rows)

    fitParamRDD.map { paramAndIndex =>
      val f = paramAndIndex._1
      val algIndex = paramAndIndex._2
      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }
      paramMap.put("fitParam", item)

      val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
      paramMap.put("kafkaParam", kafkaP.asJava)

      val tempModelLocalPath = s"/tmp/${UUID.randomUUID().toString}/${algIndex}"

      paramMap.put("internalSystemParam", Map(
        "stopFlagNum" -> stopFlagNum,
        "tempModelLocalPath" -> tempModelLocalPath
      ).asJava)

      paramMap.put("systemParam", systemParam.asJava)

      val pythonPath = systemParam.getOrElse("pythonPath", "python")


      val pythonScript = findPythonScript(userPythonScript, f, "tf")

      val res = ExternalCommandRunner.run(Seq(pythonPath, pythonScript.fileName),
        paramMap,
        MapType(StringType, MapType(StringType, StringType)),
        pythonScript.fileContent,
        pythonScript.fileName, modelPath = path, kafkaParam = kafkaParam, validateData = rowsBr.value
      )

      val score = recordUserLog(algIndex, pythonScript, kafkaParam, res)


      val fs = FileSystem.get(new Configuration())
      //delete model path
      //      if (fs.exists(new Path(path))) {
      //        throw new RuntimeException(s"please delete ${path} manually")
      //      }
      fs.delete(new Path(path), true)
      //copy
      fs.copyFromLocalFile(new Path(tempModelLocalPath),
        new Path(path))
      FileUtils.deleteDirectory(new File(tempModelLocalPath))
      ""
    }.count()
  }


  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    //val sess = TFModelLoader.load(path)
    path
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val f = (v: org.apache.spark.ml.linalg.Vector, inputName: String, outputName: String, outputSize: Int) => {
      val modelBundle = TFModelLoader.load(_model.asInstanceOf[String])
      val res = TFModelPredictor.run_float(modelBundle, inputName, outputName, outputSize, Array(v.toArray.map(f => f.toFloat)))
      Vectors.dense(res.map(f => f.toDouble))

    }
    UserDefinedFunction(f, VectorType, Some(Seq(VectorType, StringType, StringType, IntegerType)))
  }
}

object SQLTensorFlow {
  // val executors = Executors.newFixedThreadPool(3)
}
