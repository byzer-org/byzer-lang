package streaming.dsl.mmlib.algs

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.util.ExternalCommandRunner
import streaming.common.HDFSOperator
import streaming.dsl.mmlib.SQLAlg

import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 5/2/2018.
  */
class SQLSKLearn extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val (kafkaParam, newRDD) = writeKafka(df, path, params)
    val systemParam = mapParams("systemParam", params)

    val stopFlagNum = newRDD.getNumPartitions

    val fitParam = arrayParams("fitParam", params)
    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)

    val pathChunk = params("pythonDescPath").split("/")
    val userFileName = pathChunk(pathChunk.length - 1)
    val userPythonScriptList = df.sparkSession.sparkContext.textFile(params("pythonDescPath")).collect().mkString("\n")

    val wowRDD = fitParamRDD.map { f =>
      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }
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
      //读取模型文件，保存到hdfs上，方便下次获取
      val file = new File(new File(path + "_temp", "0"), "model.pickle")
      val byteArray = Files.readAllBytes(Paths.get(file.getPath))
      Row.fromSeq(Seq(byteArray))
    }
    df.sparkSession.createDataFrame(wowRDD, StructType(Seq(StructField("bytes", BinaryType)))).
      write.
      mode(SaveMode.Overwrite).
      parquet(new File(path, "0").getPath)

  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    //sparkSession.sparkContext.objectFile[Bytes]()
    path
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}
