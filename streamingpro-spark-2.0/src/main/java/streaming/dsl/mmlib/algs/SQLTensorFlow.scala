package streaming.dsl.mmlib.algs

import streaming.tensorflow.TFModelLoader
import streaming.tensorflow.TFModelPredictor
import java.io.{ByteArrayOutputStream, File}
import java.util
import com.hortonworks.spark.sql.kafka08.KafkaOperator
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
import scala.io.Source


/**
  * Created by allwefantasy on 13/1/2018.
  *
  */
class SQLTensorFlow extends SQLAlg with Functions {
  /*
       train data as TensorFlow.`/tmp/model`
        where pythonDescPath="/tmp/wow.py"
        and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
        and `kafkaParam.topic`="test-9_1517059642136"
        and `kafkaParam.group_id`="g_test-1"
        and `kafkaParam.reuse`="true"
        and `fitParam.0.epochs`="10"
        and  `fitParam.0.max_records`="10"
        and `fitParam.1.epochs`="21"
        and  `fitParam.1.max_records`="11"
        and `systemParam.pythonPath`="python";
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val (kafkaParam, newRDD) = writeKafka(df, path, params)
    val systemParam = mapParams("systemParam", params)

    val stopFlagNum = newRDD.getNumPartitions

    val fitParam = arrayParams("fitParam", params).zipWithIndex
    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)

    var userFileName = ""
    var userPythonScriptList = ""

    if (params.contains("pythonDescPath")) {
      val pathChunk = params("pythonDescPath").split("/")
      userFileName = pathChunk(pathChunk.length - 1)
      userPythonScriptList = df.sparkSession.sparkContext.textFile(params("pythonDescPath")).collect().mkString("\n")
    }
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
      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }
      paramMap.put("fitParam", item)
      paramMap.put("kafkaParam", kafkaParam.asJava)
      val tempModelLocalPath = "/tmp/train/" + WowMD5.md5Hash(path)

      paramMap.put("internalSystemParam", Map(
        "stopFlagNum" -> stopFlagNum,
        "tempModelLocalPath" -> tempModelLocalPath
      ).asJava)

      paramMap.put("systemParam", systemParam.asJava)

      val pythonPath = systemParam.getOrElse("pythonPath", "python")

      var tfSource = userPythonScriptList
      var tfName = userFileName
      if (f.contains("alg")) {
        val alg = f("alg")
        tfSource = Source.fromInputStream(ExternalCommandRunner.getClass.getResourceAsStream(s"/python/mlsql_tf_${alg}.py")).
          getLines().mkString("\n")
        tfName = s"mlsql_tf_${alg}.py"
      } else {
        require(!tfSource.isEmpty, "pythonDescPath or fitParam.0.alg is required")
      }

      val alg = f("alg")

      val res = ExternalCommandRunner.run(Seq(pythonPath, tfName),
        paramMap,
        MapType(StringType, MapType(StringType, StringType)),
        tfSource,
        tfName, modelPath = path, validateData = rowsBr.value
      )

      if (!kafkaParam.contains("userName")) {
        res.foreach(f => f)
      } else {
        val logPrefix = paramAndIndex._2 + "/" + alg + ":  "
        KafkaOperator.writeKafka(logPrefix, kafkaParam, res)
      }


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


  override def load(sparkSession: SparkSession, path: String): Any = {
    //val sess = TFModelLoader.load(path)
    path
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
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
