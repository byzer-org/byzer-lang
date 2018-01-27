package streaming.dsl.mmlib.algs

import java.io.{BufferedOutputStream, ByteArrayOutputStream, DataOutputStream, FileOutputStream}
import java.util
import java.util.Properties
import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import streaming.dsl.mmlib.SQLAlg
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.util.ExternalCommandRunner

import scala.collection.JavaConverters._


/**
  * Created by allwefantasy on 13/1/2018.
  *
  */
class SQLTensorFlow extends SQLAlg with Functions {
  /*
        train df as TF.`/tmp/cnn` where graphDefPath="python写的tf模型结构"
        and `kafkaParam.bootstrap.servers`="127.0.0.1"
        and `kafkaParam.topic` = "test.....
        and `fitParam.0.epochs` = "5"
   */
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    var kafkaParam = mapParams("kafkaParam", params)
    var systemParam = mapParams("systemParam", params)
    //    SQLTensorFlow.executors.execute(new Runnable {
    //      override def run(): Unit = {
    //
    //
    //      }
    //    })

    // we use pickler to write row to Kafka
    val structType = df.schema


    val newRDD = df.rdd.mapPartitions { iter =>
      ExternalCommandRunner.pickle(iter, structType)
    }
    val topic = kafkaParam("topic") + "_" + System.currentTimeMillis()
    kafkaParam += ("topic" -> topic)

    newRDD.foreachPartition { p =>
      val props = new Properties()
      kafkaParam.foreach(f => props.put(f._1, f._2))
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      val producer = new KafkaProducer[String, Array[Byte]](props)
      try {
        p.foreach { row =>
          producer.send(new ProducerRecord[String, Array[Byte]](topic, row))
        }

        val out = new ByteArrayOutputStream()
        ExternalCommandRunner.pickle("_stop_", out)
        val stopMsg = out.toByteArray
        out.close()

        producer.send(new ProducerRecord[String, Array[Byte]](kafkaParam("topic"), stopMsg))
      } finally {
        producer.close()
      }

    }
    val stopFlagNum = newRDD.getNumPartitions

    val fitParam = arrayParams("fitParam", params)
    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)

    val pathChunk = params("pythonDescPath").split("/")
    val userFileName = pathChunk(pathChunk.length - 1)
    val userPythonScriptList = df.sparkSession.sparkContext.textFile(params("pythonDescPath")).collect().mkString("\n")

    fitParamRDD.map { f =>
      val paramMap = new util.HashMap[String, Object]()
      val item = f.asJava
      paramMap.put("fitParam", item)
      paramMap.put("kafkaParam", kafkaParam.asJava)
      paramMap.put("internalSystemParam", Map("stopFlagNum" -> stopFlagNum).asJava)
      paramMap.put("systemParam", systemParam.asJava)

      val pythonPath = systemParam.getOrElse("pythonPath", "python")

      val res = ExternalCommandRunner.run(Seq(pythonPath, userFileName),
        paramMap,
        MapType(StringType, MapType(StringType, StringType)),
        userPythonScriptList,
        userFileName, modelPath = path
      )
      res.foreach(f => f)
      ""
    }.count()
  }


  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}

object SQLTensorFlow {
  val executors = Executors.newFixedThreadPool(3)
}
