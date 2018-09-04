package streaming.dsl.mmlib.algs

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import net.sf.json.{JSONArray, JSONObject}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.fluent.{Form, Request}
import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions => F}
import org.apache.spark.util.ObjPickle._
import org.apache.spark.util.VectorSerDer._
import org.apache.spark.util.{ExternalCommandRunner, ObjPickle, TaskContextUtil, VectorSerDer}
import streaming.common.{HDFSOperator, NetUtils}
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import streaming.dsl.mmlib.algs.tf.cluster.{ClusterSpec, ClusterStatus}
import scala.collection.mutable

import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 5/2/2018.
  */
class SQLDTFAlg extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    val enableDataLocal = params.getOrElse("enableDataLocal", "false").toBoolean

    var kafkaParam = mapParams("kafkaParam", params)

    require(kafkaParam.size > 0, "kafkaParam should be configured")

    var stopFlagNum = -1
    if (!enableDataLocal) {
      val (_kafkaParam, _newRDD) = writeKafka(df, path, params)
      stopFlagNum = _newRDD.getNumPartitions
      kafkaParam = _kafkaParam
    }

    val systemParam = mapParams("systemParam", params)
    val fitParam = arrayParamsWithIndex("fitParam", params)

    require(fitParam.size > 0, "fitParam should be configured")

    val fitParamRDD = df.sparkSession.sparkContext.parallelize(fitParam, fitParam.length)
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
    val pythonParam = systemParam.getOrElse("pythonParam", "").split(",").filterNot(f => f.isEmpty)
    var tempDataLocalPath = ""
    var dataHDFSPath = ""
    val tfDataMap = new mutable.HashMap[Int, String]()

    if (enableDataLocal) {
      val dataLocalFormat = params.getOrElse("dataLocalFormat", "json")
      val dataLocalFileNum = fitParam.filter(f => f._2("jobName") == "worker").length

      println(s"enableDataLocal is enabled. we should  prepare data for $dataLocalFileNum workers")

      dataHDFSPath = SQLPythonFunc.getAlgTmpPath(path) + "/data"
      tempDataLocalPath = SQLPythonFunc.getLocalTempDataPath(path)

      val newDF = if (dataLocalFileNum > -1) {
        df.repartition(dataLocalFileNum)
      } else df
      newDF.write.format(dataLocalFormat).mode(SaveMode.Overwrite).save(dataHDFSPath)

      HDFSOperator.listFiles(dataHDFSPath).filter(f => f.getPath.getName.endsWith(s".${dataLocalFormat}")).zipWithIndex.foreach { f =>
        val (path, index) = f
        tfDataMap.put(index, path.getPath.toUri.toString)
      }
      val printData = tfDataMap.map(f => s"${f._1}=>${f._2}").mkString("\n")
      println(s"enableDataLocal is enabled.  data partition to tensorflow worker:\n ${printData}")

    }



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

    incrementVersion(path, keepVersion)

    // create a new cluster name for tensorflow
    val clusterUuid = UUID.randomUUID().toString
    val driverHost = NetUtils.getHost
    val LIMIT = fitParam.length
    val driverPort = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].params.getOrDefault("streaming.driver.port", "9003").toString.toInt

    val wowRDD = fitParamRDD.map { paramAndIndex =>
      val f = paramAndIndex._2
      val algIndex = paramAndIndex._1

      val jobName = f("jobName")
      val taskIndex = f("taskIndex").toInt

      def isPs = {
        jobName == "ps"
      }
      val roleSpec = new JSONObject()
      roleSpec.put("jobName", jobName)
      roleSpec.put("taskIndex", taskIndex)


      val host = NetUtils.getHost
      val holdPort = NetUtils.availableAndReturn(ClusterSpec.MIN_PORT_NUMBER, ClusterSpec.MAX_PORT_NUMBER)

      if (holdPort == null) {
        throw new RuntimeException(s"Fail to create tensorflow cluster, maybe executor cannot bind port ")
      }

      val port = holdPort.getLocalPort

      def reportToMaster(path: String = "/cluster/register") = {
        Request.Post(s"http://${driverHost}:${driverPort}${path}").bodyForm(Form.form().add("cluster", clusterUuid).
          add("hostAndPort", s"${host}:${port}")
          .add("jobName", f("jobName"))
          .add("taskIndex", f("taskIndex"))
          .build())
          .execute().returnContent().asString()
      }

      def fetchClusterFromMaster = {
        Request.Get(s"http://${driverHost}:${driverPort}/cluster?cluster=${clusterUuid}")
          .execute().returnContent().asString()
      }

      def getHostAndPortFromJson(infos: JSONArray, t: String) = {
        infos.asScala.map(f => f.asInstanceOf[JSONObject]).filter(f => f.getString("jobName") == t).map(f => s"${f.getString("host")}:${f.getInt("port")}")
      }

      // here we should report to driver
      reportToMaster()
      // wait until all workers have been registered
      var waitCount = 0
      val maxWaitCount = 100
      var noWait = false
      val clusterSpecRef = new AtomicReference[ClusterSpec]()
      while (!noWait && waitCount < maxWaitCount) {
        val response = fetchClusterFromMaster
        val infos = JSONArray.fromObject(response)
        println(s"Waiting all worker/ps started. Wait times: ${waitCount} times. Already registered tf worker/ps: ${infos.size()}")
        if (infos.size == LIMIT) {
          val workerList = getHostAndPortFromJson(infos, "worker")
          val psList = getHostAndPortFromJson(infos, "ps")
          clusterSpecRef.set(new ClusterSpec(workerList.toList, psList.toList))
          noWait = true
        } else {
          Thread.sleep(5000)
          waitCount += 1
        }
      }

      NetUtils.releasePort(holdPort)
      if (clusterSpecRef.get() == null) {
        throw new RuntimeException(s"Fail to create tensorflow cluster, this maybe caused by  executor cannot connect driver at ${driverHost}:${driverPort}")
      }

      val clusterSpec = clusterSpecRef.get()


      val clusterSpecJson = new JSONObject()
      clusterSpecJson.put("worker", clusterSpec.worker.asJava)
      clusterSpecJson.put("ps", clusterSpec.ps.asJava)

      println(clusterSpecJson.toString)

      var tempDataLocalPathWithAlgSuffix = tempDataLocalPath



      if (!isPs && enableDataLocal) {
        val partitionDataInHDFS = tfDataMap(algIndex)
        tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
        println(s"Copy HDFS ${partitionDataInHDFS} to local ${tempDataLocalPathWithAlgSuffix} ")
        recordSingleLineLog(kafkaParam, s"Copy HDFS ${partitionDataInHDFS} to local ${tempDataLocalPathWithAlgSuffix} ")
        HDFSOperator.copyToLocalFile(tempLocalPath = tempDataLocalPathWithAlgSuffix + "/" + partitionDataInHDFS.split("/").last, path = partitionDataInHDFS, true)
      }

      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }

      // load resource
      var resourceParams = Map.empty[String, String]
      if (f.keys.map(_.split("\\.")(0)).toSet.contains("resource")) {
        val resources = Functions.mapParams(s"resource", f)
        resources.foreach {
          case (resourceName, resourcePath) =>
            val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
            recordSingleLineLog(kafkaParam, s"resource paramter found,system will load resource ${resourcePath} in ${tempResourceLocalPath} in executor.")
            HDFSOperator.copyToLocalFile(tempResourceLocalPath, resourcePath, true)
            resourceParams += (resourceName -> tempResourceLocalPath)
            recordSingleLineLog(kafkaParam, s"resource loaded.")
        }
      }

      val pythonScript = findPythonScript(userPythonScript, f, "sk")

      val tempModelLocalPath = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      val checkpointDir = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      //FileUtils.forceMkdir(tempModelLocalPath)

      paramMap.put("fitParam", item)

      val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
      paramMap.put("kafkaParam", kafkaP.asJava)

      val internalSystemParam = Map(
        "stopFlagNum" -> stopFlagNum,
        "tempModelLocalPath" -> tempModelLocalPath,
        "tempDataLocalPath" -> tempDataLocalPathWithAlgSuffix,
        "resource" -> resourceParams.asJava,
        "clusterSpec" -> clusterSpecJson.toString(),
        "roleSpec" -> roleSpec.toString(),
        "checkpointDir" -> checkpointDir
      )

      paramMap.put("internalSystemParam", internalSystemParam.asJava)
      paramMap.put("systemParam", systemParam.asJava)


      val command = Seq(pythonPath) ++ pythonParam ++ Seq(pythonScript.fileName)

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var trainFailFlag = false

      if (!isPs) {
        try {
          val res = ExternalCommandRunner.run(
            command = command,
            iter = paramMap,
            schema = MapType(StringType, MapType(StringType, StringType)),
            scriptContent = pythonScript.fileContent,
            scriptName = pythonScript.fileName,
            recordLog = SQLPythonFunc.recordAnyLog(kafkaParam),
            modelPath = path, validateData = rowsBr.value
          )

          score = recordUserLog(algIndex, pythonScript, kafkaParam, res)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            trainFailFlag = true
        }
      } else {
        val pythonWorker = new AtomicReference[Process]()
        val context = TaskContext.get()

        // notice: I still get no way how to destroy the ps python worker
        // when spark existed unexpectedly.
        // we have handle the situation eg. task is cancel or
        val pythonPSThread = new Thread(new Runnable {
          override def run(): Unit = {
            TaskContextUtil.setContext(context)
            try {
              val res = ExternalCommandRunner.run(
                command = command,
                iter = paramMap,
                schema = MapType(StringType, MapType(StringType, StringType)),
                scriptContent = pythonScript.fileContent,
                scriptName = pythonScript.fileName,
                recordLog = SQLPythonFunc.recordAnyLog(kafkaParam),
                modelPath = path, validateData = rowsBr.value
              )
              pythonWorker.set(res.asInstanceOf[ {def getWorker: Process}].getWorker)
              score = recordUserLog(algIndex, pythonScript, kafkaParam, res)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                trainFailFlag = true
            }
          }
        })
        pythonPSThread.setDaemon(true)
        pythonPSThread.start()


        //        val pythonPSMonitorThread = new Thread(new Runnable {
        //          override def run(): Unit = {
        //            while (!context.isInterrupted && !context.isCompleted) {
        //              Thread.sleep(2000)
        //            }
        //            if (context.isCompleted || context.isInterrupted()) {
        //              try {
        //                pythonWorker.get().destroy()
        //              } catch {
        //                case e: Exception =>
        //              }
        //            }
        //          }
        //        })
        //        pythonPSMonitorThread.setDaemon(true)
        //        pythonPSMonitorThread.start()

        var shouldWait = true

        def fetchClusterStatusFromMaster = {
          Request.Get(s"http://${driverHost}:${driverPort}/cluster/worker/finish?cluster=${clusterUuid}")
            .execute().returnContent().asString()
        }

        // PS should wait until
        // all worker finish their jobs.
        // PS python worker use thread.join to keep it alive .
        while (shouldWait) {
          val response = fetchClusterStatusFromMaster
          if (response.toInt == clusterSpec.worker.size) {
            shouldWait = false
          }
          try {
            Thread.sleep(10000)
          } catch {
            case e: Exception =>
              shouldWait = false
          }
          println(s"check worker finish size. targetSize:${clusterSpec.worker.size} vs ${response.toInt}")
        }
        pythonWorker.get().destroy()
        pythonPSThread.interrupt()

      }
      val modelTrainEndTime = System.currentTimeMillis()
      if (!isPs) {
        if (!trainFailFlag) {
          reportToMaster("/cluster/worker/status")
        }
        val modelTrainEndTime = System.currentTimeMillis()

        val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
        try {
          //模型保存到hdfs上
          val fs = FileSystem.get(new Configuration())
          if (!keepVersion) {
            fs.delete(new Path(modelHDFSPath), true)
          }
          if (new File(tempModelLocalPath).exists()) {
            fs.copyFromLocalFile(new Path(tempModelLocalPath),
              new Path(modelHDFSPath))
          } else {
            if (new File(checkpointDir).exists()) {
              fs.copyFromLocalFile(new Path(checkpointDir),
                new Path(modelHDFSPath))
            }
          }

        } catch {
          case e: Exception =>
            trainFailFlag = true
        } finally {
          // delete local model
          FileUtils.deleteDirectory(new File(tempModelLocalPath))
          // delete local data
          FileUtils.deleteDirectory(new File(tempDataLocalPathWithAlgSuffix))
          FileUtils.deleteDirectory(new File(checkpointDir))
        }
        val status = if (trainFailFlag) "fail" else "success"
        Row.fromSeq(Seq(modelHDFSPath, algIndex, pythonScript.fileName, score, status, modelTrainStartTime, modelTrainEndTime, f))
      } else {
        val status = if (trainFailFlag) "fail" else "success"
        Row.fromSeq(Seq("", algIndex, pythonScript.fileName, score, status, modelTrainStartTime, modelTrainEndTime, f))
      }
    }
    df.sparkSession.createDataFrame(wowRDD, StructType(Seq(
      StructField("modelPath", StringType),
      StructField("algIndex", IntegerType),
      StructField("alg", StringType),
      StructField("score", DoubleType),

      StructField("status", StringType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("trainParams", MapType(StringType, StringType))
    ))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")

    val tempRDD = df.sparkSession.sparkContext.parallelize(Seq(Seq(Map("pythonPath" -> pythonPath, "pythonVer" -> pythonVer), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    df.sparkSession.createDataFrame(tempRDD, StructType(Seq(
      StructField("systemParam", MapType(StringType, StringType)),
      StructField("trainParams", MapType(StringType, StringType))))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/1")

  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    val maxVersion = getModelVersion(_path)
    val versionEnabled = maxVersion match {
      case Some(v) => true
      case None => false
    }

    val modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

    // you can specify model version
    val path = if (modelVersion == -1) getAlgMetalPath(_path, versionEnabled)
    else getAlgMetalPathWithVersion(_path, modelVersion)

    val modelPath = if (modelVersion == -1) getAlgModelPath(_path, versionEnabled)
    else getAlgModelPathWithVersion(_path, modelVersion)

    var algIndex = params.getOrElse("algIndex", "-1").toInt

    val modelList = sparkSession.read.parquet(path + "/0").collect()
    val models = if (algIndex != -1) {
      Seq(modelPath + "/" + algIndex)
    } else {
      modelList.map(f => (f(3).asInstanceOf[Double], f(0).asInstanceOf[String], f(1).asInstanceOf[Int]))
        .toSeq
        .sortBy(f => f._1)(Ordering[Double].reverse)
        .take(1)
        .map(f => {
          algIndex = f._3
          modelPath + "/" + f._2.split("/").last
        })
    }

    import sparkSession.implicits._
    val wowMetas = sparkSession.read.parquet(path + "/1").collect()
    var trainParams = Map[String, String]()

    def getTrainParams(isNew: Boolean) = {
      if (isNew)
        wowMetas.map(f => f.getMap[String, String](1)).head.toMap
      else {
        val df = sparkSession.read.parquet(MetaConst.PARAMS_PATH(path, "params")).map(f => (f.getString(0), f.getString(1)))
        df.collect().toMap
      }
    }

    if (versionEnabled) {
      trainParams = getTrainParams(true)
    }

    try {
      trainParams = getTrainParams(false)
    } catch {
      case e: Exception =>
        logInfo(format(s"no directory: ${MetaConst.PARAMS_PATH(path, "params")} ; using ${path + "/1"}"))
        trainParams = getTrainParams(true)
    }


    // load resource
    val fitParam = arrayParamsWithIndex("fitParam", trainParams)
    val selectedFitParam = fitParam(algIndex)._2
    val loadResource = selectedFitParam.keys.map(_.split("\\.")(0)).toSet.contains("resource")
    var resourceParams = Map.empty[String, String]

    val metas = wowMetas.map(f => f.getMap[String, String](0)).toSeq
    // make sure every executor have the model in local directory.
    // we should unregister manually
    models.foreach { modelPath =>
      val tempModelLocalPath = SQLPythonFunc.getLocalTempModelPath(modelPath)
      distributeResource(sparkSession, modelPath, tempModelLocalPath)

      if (loadResource) {
        val resources = Functions.mapParams(s"resource", selectedFitParam)
        resources.foreach {
          case (resourceName, resourcePath) =>
            val tempResourceLocalPath = SQLPythonFunc.getLocalTempResourcePath(resourcePath, resourceName)
            resourceParams += (resourceName -> tempResourceLocalPath)
            distributeResource(sparkSession, resourcePath, tempResourceLocalPath)
        }
      }
    }

    (models, metas, trainParams, selectedFitParam + ("resource" -> resourceParams.asJava))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val (modelsTemp, metasTemp, trainParams, selectedFitParam) =
      _model.asInstanceOf[(Seq[String], Seq[Map[String, String]], Map[String, String], Map[String, Any])]
    val models = sparkSession.sparkContext.broadcast(modelsTemp)

    val runtimeParams = PlatformManager.getRuntime.params

    val pythonPath = metasTemp(0)("pythonPath")
    val pythonVer = metasTemp(0)("pythonVer")

    val userPythonScript = findPythonPredictScript(sparkSession, params, "")

    val maps = new util.HashMap[String, java.util.Map[String, _]]()
    val item = new util.HashMap[String, String]()
    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
    maps.put("systemParam", item)
    maps.put("internalSystemParam", selectedFitParam.asJava)

    val kafkaParam = mapParams("kafkaParam", trainParams)

    val enableCopyTrainParamsToPython = params.getOrElse("enableCopyTrainParamsToPython", "false").toBoolean
    //driver 节点执行
    val res = ExternalCommandRunner.run(Seq(pythonPath, userPythonScript.fileName),
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      userPythonScript.fileContent,
      userPythonScript.fileName, modelPath = null, recordLog = SQLPythonFunc.recordAnyLog(kafkaParam)
    )
    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))

    val enableErrorMsgToKafka = params.getOrElse("enableErrorMsgToKafka", "false").toBoolean
    val kafkaParam2 = if (enableErrorMsgToKafka) kafkaParam else Map[String, String]()


    val f = (v: org.apache.spark.ml.linalg.Vector, modelPath: String) => {
      val modelRow = InternalRow.fromSeq(Seq(SQLPythonFunc.getLocalTempModelPath(modelPath)))
      val trainParamsRow = InternalRow.fromSeq(Seq(ArrayBasedMapData(trainParams)))
      val v_ser = pickleInternalRow(Seq(ser_vector(v)).toIterator, vector_schema())
      val v_ser2 = pickleInternalRow(Seq(modelRow).toIterator, StructType(Seq(StructField("modelPath", StringType))))
      var v_ser3 = v_ser ++ v_ser2
      if (enableCopyTrainParamsToPython) {
        val v_ser4 = pickleInternalRow(Seq(trainParamsRow).toIterator, StructType(Seq(StructField("trainParams", MapType(StringType, StringType)))))
        v_ser3 = v_ser3 ++ v_ser4
      }

      //      val predictTime = System.currentTimeMillis()
      val recordLog = SQLPythonFunc.recordAnyLog(kafkaParam2)
      val iter = WowPythonRunner.run(
        pythonPath, pythonVer, command, v_ser3, TaskContext.get().partitionId(), Array(), runtimeParams.asScala.toMap, recordLog
      )
      val a = iter.next()
      val predictValue = VectorSerDer.deser_vector(unpickle(a).asInstanceOf[java.util.ArrayList[Object]].get(0))
      //      val predictEndTime = System.currentTimeMillis()
      //      println("")
      predictValue
    }

    val f2 = (v: org.apache.spark.ml.linalg.Vector) => {
      models.value.map { modelPath =>
        val resV = f(v, modelPath)
        (resV(resV.argmax), resV)
      }.sortBy(f => f._1).reverse.head._2
    }

    UserDefinedFunction(f2, VectorType, Some(Seq(VectorType)))
  }

}
