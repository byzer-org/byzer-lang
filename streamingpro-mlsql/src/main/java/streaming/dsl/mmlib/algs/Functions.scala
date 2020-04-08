/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import java.io.ByteArrayOutputStream
import java.util.Properties

import net.csdn.common.reflect.ReflectHelper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, MLSQLUtils, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.util.{ObjPickle, WowXORShiftRandom}
import org.apache.spark.{MLSQLConf, Partitioner, SparkConf}
import streaming.core.message.MLSQLMessage
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.log.WowLog
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.ArrayBuffer

/**
 * Created by allwefantasy on 13/1/2018.
 */
trait Functions extends SQlBaseFunc with Logging with WowLog with Serializable {

  def pythonCheckRequirements(df: DataFrame) = {
    val conf: SparkConf = df.sparkSession.sparkContext.getConf
    val master = conf.get("spark.master", "")
    val isLocalMaster = (master == "local" || master.startsWith("local["))
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]

    if (!isLocalMaster && !MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.readFrom(runtime.configReader)) {
      throw new MLSQLException(MLSQLMessage.PYTHON_REQUEIRE_MLSQL_CLUSTER_PS_ENABLE)
    }
  }

  def emptyDataFrame()(implicit df: DataFrame) = {
    import df.sparkSession.implicits._
    Seq.empty[String].toDF("name")
  }

  def emptyDataFrame(spark: SparkSession, name: String) = {
    import spark.implicits._
    Seq.empty[String].toDF(name)
  }

  def sampleUnbalanceWithMultiModel(df: DataFrame, path: String, params: Map[String, String], train: (DataFrame, Int) => Unit) = {
    //select count(*) as subLabelCount,label from _ group by labelCol order by  subLabelCount asc
    val labelCol = params.getOrElse("labelCol", "label")
    val labelToCountSeq = df.groupBy(labelCol).agg(F.count(labelCol).as("subLabelCount")).orderBy(F.asc("subLabelCount")).
      select(labelCol, "subLabelCount").collect().map { f =>
      (f.getDouble(0), f.getLong(1))
    }
    val forLog = labelToCountSeq.map(f => s"${f._1}:${f._2}").mkString(",")
    logInfo(format(s"computing data stat:${forLog}"))
    val labelCount = labelToCountSeq.size

    val dfWithLabelPartition = df.rdd.map { f =>
      (f.getAs[Double](labelCol).toInt, f)
    }.partitionBy(new Partitioner {
      override def numPartitions: Int = labelCount

      override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int]
      }
    }).cache()

    try {
      val minLabel = labelToCountSeq.head._1
      val minCount = labelToCountSeq.head._2

      val maxLabel = labelToCountSeq.last._1
      val maxCount = labelToCountSeq.last._2

      val times = (maxCount.toDouble / minCount).ceil

      val labelToCountMapBr = df.sparkSession.sparkContext.broadcast(labelToCountSeq.map { f =>
        //sample rate
        (f._1, minCount.toDouble / f._2)
      }.toMap)
      val forLog2 = labelToCountMapBr.value.map(f => s"${f._1}:${f._2}").mkString(",")
      logInfo(format(s"all label sample rate:${forLog2}"))

      (0 until times.toInt).foreach { time =>
        val tempRdd = dfWithLabelPartition.mapPartitionsWithIndex { (label, iter) =>
          val wow = new WowXORShiftRandom()
          iter.filter(k => wow.nextDouble <= labelToCountMapBr.value(label)).map(f => f._2)
        }
        val trainData = df.sparkSession.createDataFrame(tempRdd, df.schema)
        logInfo(format(s"training model :${time}"))
        train(trainData, time)
      }
    } finally {
      dfWithLabelPartition.unpersist(false)
    }
  }

  def configureModel(model: Params, params: Map[String, String]) = {
    model.params.map { f =>
      if (params.contains(f.name)) {
        val v = params(f.name)
        val m = model.getClass.getMethods.filter(m => m.getName == s"set${f.name.capitalize}").head
        val pt = m.getParameterTypes.head
        val v2 = pt match {
          case i if i.isAssignableFrom(classOf[Int]) => v.toInt
          case i if i.isAssignableFrom(classOf[Double]) => v.toDouble
          case i if i.isAssignableFrom(classOf[Float]) => v.toFloat
          case i if i.isAssignableFrom(classOf[Long]) => v.toLong
          case i if i.isAssignableFrom(classOf[Boolean]) => v.toBoolean
          case i if i.isAssignableFrom(classOf[String]) => v
          case i if i.isAssignableFrom(classOf[Array[Int]]) => v.split(",").map(_.toInt)
          case i if i.isAssignableFrom(classOf[Array[Double]]) => v.split(",").map(_.toDouble)
          case i if i.isAssignableFrom(classOf[Array[Float]]) => v.split(",").map(_.toFloat)
          case i if i.isAssignableFrom(classOf[Array[Long]]) => v.split(",").map(_.toLong)
          case i if i.isAssignableFrom(classOf[Array[Boolean]]) => v.split(",").map(_.toBoolean)
          case i if i.isAssignableFrom(classOf[Array[String]]) => v.split(",")
          case _ => logWarning(format(s"Can not assign value to model: ${f.name} -> ${v}"))
        }
        m.invoke(model, v2.asInstanceOf[AnyRef])
      }
    }
  }

  def mapParams(name: String, params: Map[String, String]) = {
    Functions.mapParams(name, params)
  }

  def arrayParams(name: String, params: Map[String, String]) = {
    params.filter(f => f._1.startsWith(name + ".")).map { f =>
      val Array(name, group, keys@_*) = f._1.split("\\.")
      (group, keys.mkString("."), f._2)
    }.groupBy(f => f._1).map { f =>
      f._2.map(k =>
        (k._2, k._3)).toMap
    }.toArray
  }

  def arrayParamsWithIndex(name: String, params: Map[String, String]): Array[(Int, Map[String, String])] = {
    params.filter(f => f._1.startsWith(name + ".")).map { f =>
      val Array(name, group, keys@_*) = f._1.split("\\.")
      (group, keys.mkString("."), f._2)
    }.groupBy(f => f._1).map(f => {
      val params = f._2.map(k => (k._2, k._3)).toMap
      (f._1.toInt, params)
    }).toArray
  }

  def getModelConstructField(model: Any, modelName: String, fieldName: String) = {
    val modelField = model.getClass.getDeclaredField("org$apache$spark$ml$feature$" + modelName + "$$" + fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }

  def getModelField(model: Any, fieldName: String) = {
    val modelField = model.getClass.getDeclaredField(fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }

  def loadModels(path: String, modelType: (String) => Any) = {
    val files = HDFSOperator.listModelDirectory(path).filterNot(_.getPath.getName.startsWith("__"))
    val models = ArrayBuffer[Any]()
    files.foreach { f =>
      val model = modelType(f.getPath.toString)
      models += model
    }
    models
  }

  def trainModels[T <: Model[T]](df: DataFrame, path: String, params: Map[String, String], modelType: () => Params) = {

    def f(trainData: DataFrame, modelIndex: Int) = {
      val alg = modelType()
      configureModel(alg, params)
      val model = alg.asInstanceOf[Estimator[T]].fit(trainData)
      model.asInstanceOf[MLWritable].write.overwrite().save(path + "/" + modelIndex)
    }

    params.getOrElse("multiModels", "false").toBoolean match {
      case true => sampleUnbalanceWithMultiModel(df, path, params, f)
      case false =>
        f(df, 0)
    }
  }


  def trainModelsWithMultiParamGroup2(df: DataFrame, path: String, params: Map[String, String],
                                      modelType: () => Params,
                                      evaluate: (Params, Map[String, String]) => List[MetricValue]
                                     ) = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    val mf = (trainData: DataFrame, fitParam: Map[String, String], modelIndex: Int) => {
      val alg = modelType()
      configureModel(alg, fitParam)

      logInfo(format(s"[training] [alg=${alg.getClass.getName}] [keepVersion=${keepVersion}]"))

      var status = "success"
      val modelTrainStartTime = System.currentTimeMillis()
      val modelPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + modelIndex
      var scores: List[MetricValue] = List()
      try {
        val model = ReflectHelper.method(alg, "fit", trainData)
        model.asInstanceOf[MLWritable].write.overwrite().save(modelPath)
        scores = evaluate(model.asInstanceOf[Params], fitParam)
        logInfo(format(s"[trained] [alg=${alg.getClass.getName}] [metrics=${scores}] [model hyperparameters=${
          model.asInstanceOf[Params].explainParams().replaceAll("\n", "\t")
        }]"))
      } catch {
        case e: Exception =>
          logInfo(format_exception(e))
          status = "fail"
      }
      val modelTrainEndTime = System.currentTimeMillis()
      //      if (status == "fail") {
      //        throw new RuntimeException(s"Fail to train als model: ${modelIndex}; All will fails")
      //      }
      val metrics = scores.map(score => Row.fromSeq(Seq(score.name, score.value))).toArray
      Row.fromSeq(Seq(modelPath, modelIndex, alg.getClass.getName, metrics, status, modelTrainStartTime, modelTrainEndTime, fitParam))
    }
    var fitParam = arrayParamsWithIndex("fitParam", params)
    if (fitParam.size == 0) {
      fitParam = Array((0, Map[String, String]()))
    }

    val wowRes = fitParam.map { fp =>
      mf(df, fp._2, fp._1)
    }

    val wowRDD = df.sparkSession.sparkContext.parallelize(wowRes, 1)

    df.sparkSession.createDataFrame(wowRDD, StructType(Seq(
      StructField("modelPath", StringType),
      StructField("algIndex", IntegerType),
      StructField("alg", StringType),
      StructField("metrics", ArrayType(StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = DoubleType)
      )))),

      StructField("status", StringType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("trainParams", MapType(StringType, StringType))
    ))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }

  def trainModelsWithMultiParamGroup[T <: Model[T]](df: DataFrame, path: String, params: Map[String, String],
                                                    modelType: () => Params,
                                                    evaluate: (Params, Map[String, String]) => List[MetricValue]
                                                   ) = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    val mf = (trainData: DataFrame, fitParam: Map[String, String], modelIndex: Int) => {
      val alg = modelType()
      configureModel(alg, fitParam)

      logInfo(format(s"[training] [alg=${alg.getClass.getName}] [keepVersion=${keepVersion}]"))

      var status = "success"
      var info = ""
      val modelTrainStartTime = System.currentTimeMillis()
      val modelPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + modelIndex
      var scores: List[MetricValue] = List()
      try {
        val model = alg.asInstanceOf[Estimator[T]].fit(trainData)
        model.asInstanceOf[MLWritable].write.overwrite().save(modelPath)
        scores = evaluate(model, fitParam)
        logInfo(format(s"[trained] [alg=${alg.getClass.getName}] [metrics=${scores}] [model hyperparameters=${
          model.asInstanceOf[Params].explainParams().replaceAll("\n", "\t")
        }]"))
      } catch {
        case e: Exception =>
          info = format_exception(e)
          logInfo(info)
          status = "fail"
      }
      val modelTrainEndTime = System.currentTimeMillis()
      //      if (status == "fail") {
      //        throw new RuntimeException(s"Fail to train als model: ${modelIndex}; All will fails")
      //      }
      val metrics = scores.map(score => Row.fromSeq(Seq(score.name, score.value))).toArray
      Row.fromSeq(Seq(
        modelPath.substring(path.length),
        modelIndex,
        alg.getClass.getName,
        metrics,
        status,
        info,
        modelTrainStartTime,
        modelTrainEndTime,
        fitParam))
    }
    var fitParam = arrayParamsWithIndex("fitParam", params)

    if (fitParam.size == 0) {
      fitParam = Array((0, Map[String, String]()))
    }

    val wowRes = fitParam.map { fp =>
      mf(df, fp._2, fp._1)
    }

    val wowRDD = df.sparkSession.sparkContext.parallelize(wowRes, 1)

    df.sparkSession.createDataFrame(wowRDD, StructType(Seq(
      StructField("modelPath", StringType),
      StructField("algIndex", IntegerType),
      StructField("alg", StringType),
      StructField("metrics", ArrayType(StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = DoubleType)
      )))),

      StructField("status", StringType),
      StructField("message", StringType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("trainParams", MapType(StringType, StringType))
    ))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }

  def predict_classification(sparkSession: SparkSession, _model: Any, name: String) = {

    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[Any]])

    val raw2probabilityMethod = if (sparkSession.version.startsWith("2.3")) "raw2probabilityInPlace" else "raw2probability"

    val f = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        //概率，分类
        (raw2probability(raw2probability.argmax), raw2probability)
      }.sortBy(f => f._1).reverse.head._2
    }

    val f2 = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        raw2probability
      }
    }

    sparkSession.udf.register(name + "_raw", f2)

    MLSQLUtils.createUserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }

  def writeKafka(df: DataFrame, path: String, params: Map[String, String]) = {
    var kafkaParam = mapParams("kafkaParam", params)
    // we use pickler to write row to Kafka
    val structType = df.schema

    val newRDD = df.rdd.mapPartitions { iter =>
      ObjPickle.pickle(iter, structType)
    }
    val topic = kafkaParam("topic") + "_" + System.currentTimeMillis()
    if (!kafkaParam.getOrElse("reuse", "false").toBoolean) {
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

          def pickle(msg: String) = {
            val out = new ByteArrayOutputStream()
            ObjPickle.pickle(msg, out)
            val stopMsg = out.toByteArray
            out.close()
            stopMsg
          }

          val stopMsg = pickle("_stop_")
          producer.send(new ProducerRecord[String, Array[Byte]](kafkaParam("topic"), stopMsg))
        } finally {
          producer.close()
        }

      }
    }
    (kafkaParam, newRDD)
  }


  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    HDFSOperator.createTempModelLocalPath(path, autoCreateParentDir)
  }


  def distributeResource(spark: SparkSession, path: String, tempLocalPath: String) = {
    val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].psDriverBackend
    psDriverBackend.psDriverRpcEndpointRef.askSync[Boolean](Message.CopyModelToLocal(path, tempLocalPath))
  }
}

object Functions {
  def mapParams(name: String, params: Map[String, String]) = {
    //    params.filter(f => f._1.startsWith(name + ".")).map(f => (f._1.split("\\.").drop(1).mkString("."), f._2))
    params.filter(f => f._1.startsWith(name + ".")).map(f => (f._1.substring(name.length + 1, f._1.length), f._2))
  }
}