package tech.mlsql.ets.tensorflow

import java.io.{DataInputStream, DataOutputStream, File}
import java.net.Socket
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.util.{ExternalCommandRunner, ObjPickle, TaskContextUtil}
import org.apache.spark.{MLSQLSparkUtils, TaskContext}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import streaming.dsl.mmlib.algs.{Functions, SQLPythonFunc}
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.ets.tensorflow.servers._

import scala.collection.JavaConverters._
import scala.collection.mutable

class DistributedTensorflow extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    // We start a temp driver server, so the workers can connect it.
    // It will be used to coordinate the ps workers.

    val tfDriver = new TFDriver[String](new AtomicReference[String]("")) {
      override def host: String = MLSQLSparkUtils.rpcEnv().address.host
    }

    val tempSocketServerHost = tfDriver._host
    val tempSocketServerPort = tfDriver._port


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    val enableDataLocal = params.getOrElse("enableDataLocal", "false").toBoolean

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
      emptyDataFrame()(df)

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

    val LIMIT = fitParam.length

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

      val MIN_PORT_NUMBER = 2221
      val MAX_PORT_NUMBER = 6666
      val holdPort = NetUtils.availableAndReturn(MIN_PORT_NUMBER, MAX_PORT_NUMBER)

      if (holdPort == null) {
        throw new RuntimeException(s"Fail to create tensorflow cluster, maybe executor cannot bind port ")
      }

      val port = holdPort.getLocalPort


      val tfClient = new TFClient()
      val socket = new Socket(tempSocketServerHost, tempSocketServerPort)
      val driverOutputStream = new DataOutputStream(socket.getOutputStream)
      val driverInputStream = new DataInputStream(socket.getInputStream)


      // here we should report to driver
      def reportToMaster() = {
        tfClient.sendRequest(driverOutputStream, ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port, jobName, taskIndex, isPs))
        tfClient.readResponse(driverInputStream).asInstanceOf[ReportToMasterResponse]
      }

      reportToMaster()

      def fetchClusterSpec(): ClusterSpecResponse = {
        tfClient.sendRequest(driverOutputStream, ClusterSpecRequest())
        tfClient.readResponse(driverInputStream).asInstanceOf[ClusterSpecResponse]
      }

      // wait until all worker/ps have been registered
      var waitCount = 0
      val maxWaitCount = 100
      var noWait = false
      val clusterSpecRef = new AtomicReference[ClusterSpecResponse]()
      while (!noWait && waitCount < maxWaitCount) {
        val response = fetchClusterSpec()
        val totalRegistered = response.workerTasks.size + response.psTasks.size
        logInfo(
          s"""
             |
             |----------------------------------
             |Waiting all worker/ps started
             |__________________________________
             |Wait times: ${waitCount} times.
             |Target: ${LIMIT}
             |totalRegistered: ${totalRegistered}
             |
             |PS: ${response.ps}
             |Workers: ${response.workers}
             |
             |
           """.stripMargin)
        if (totalRegistered == LIMIT) {
          clusterSpecRef.set(response)
          noWait = true
        } else {
          Thread.sleep(5000)
          waitCount += 1
        }
      }

      if (clusterSpecRef.get() == null) {
        throw new RuntimeException(s"Fail to create tensorflow cluster, this maybe caused by  executor cannot connect driver at ${tempSocketServerHost}:${tempSocketServerPort}")
      }

      val clusterSpec = clusterSpecRef.get()
      val clusterSpecJson = new JSONObject()
      clusterSpecJson.put("worker", clusterSpec.workers.asJava)
      clusterSpecJson.put("ps", clusterSpec.ps.asJava)

      def reportSuccess = {
        tfClient.sendRequest(driverOutputStream, JobStatusRequest(jobName, taskIndex, true, true))
        tfClient.readResponse(driverInputStream).asInstanceOf[JobStatusResponse]
      }

      def reportFail = {
        tfClient.sendRequest(driverOutputStream, JobStatusRequest(jobName, taskIndex, true, false))
        tfClient.readResponse(driverInputStream).asInstanceOf[JobStatusResponse]
      }


      var tempDataLocalPathWithAlgSuffix = tempDataLocalPath


      if (!isPs && enableDataLocal) {
        val partitionDataInHDFS = tfDataMap(algIndex)
        tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
        logInfo(s"Copy HDFS ${partitionDataInHDFS} to local ${tempDataLocalPathWithAlgSuffix} ")
        logInfo(s"Copy HDFS ${partitionDataInHDFS} to local ${tempDataLocalPathWithAlgSuffix} ")
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
            logInfo(s"resource paramter found,system will load resource ${resourcePath} in ${tempResourceLocalPath} in executor.")
            HDFSOperator.copyToLocalFile(tempResourceLocalPath, resourcePath, true)
            resourceParams += (resourceName -> tempResourceLocalPath)
        }
      }

      val pythonScript = userPythonScript.get

      val tempModelLocalPath = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      val checkpointDir = s"${SQLPythonFunc.getLocalBasePath}/${UUID.randomUUID().toString}/${algIndex}"
      //FileUtils.forceMkdir(tempModelLocalPath)

      paramMap.put("fitParam", item)


      val internalSystemParam = Map(
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
      val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var trainFailFlag = false

      NetUtils.releasePort(holdPort)
      if (!isPs) {
        try {

          val res = ExternalCommandRunner.run(taskDirectory = taskDirectory,
            command = command,
            iter = paramMap,
            schema = MapType(StringType, MapType(StringType, StringType)),
            scriptContent = pythonScript.fileContent,
            scriptName = pythonScript.fileName,
            recordLog = SQLPythonFunc.recordAnyLog(Map()),
            modelPath = path, validateData = rowsBr.value
          )

          score = recordUserLog(algIndex, pythonScript, Map(), res)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            reportFail
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
            val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)
            try {
              val res = ExternalCommandRunner.run(taskDirectory = taskDirectory,
                command = command,
                iter = paramMap,
                schema = MapType(StringType, MapType(StringType, StringType)),
                scriptContent = pythonScript.fileContent,
                scriptName = pythonScript.fileName,
                recordLog = SQLPythonFunc.recordAnyLog(Map()),
                modelPath = path, validateData = rowsBr.value
              )
              pythonWorker.set(res.asInstanceOf[ {def getWorker: Process}].getWorker)
              score = recordUserLog(algIndex, pythonScript, Map(), res)
            } catch {
              case e: Exception =>
                e.printStackTrace()
                reportFail
                trainFailFlag = true
            }
          }
        })
        pythonPSThread.setDaemon(true)
        pythonPSThread.start()

        var shouldWait = true

        def fetchClusterStatusFromMaster = {
          tfClient.sendRequest(driverOutputStream, JobStatusRequest(jobName, taskIndex, false, false))
          tfClient.readResponse(driverInputStream).asInstanceOf[JobStatusResponse]
        }

        // PS should wait until
        // all worker finish their jobs.
        // PS python worker use thread.join to keep it alive .
        while (shouldWait) {
          val response = fetchClusterStatusFromMaster
          val workSize = response.jobStatus.filter(f => f.done && !f.isPs).size
          if (workSize == clusterSpec.workers.size) {
            shouldWait = false
          }
          try {
            Thread.sleep(10000)
          } catch {
            case e: Exception =>
              shouldWait = false
          }
          logInfo(
            s"""
               |PS check worker is all finished.
               |targetSize:  ${clusterSpec.workers.size}
               |currentSize: ${workSize}
            """.stripMargin)
        }
        pythonWorker.get().destroy()
        pythonPSThread.interrupt()

      }
      val modelTrainEndTime = System.currentTimeMillis()
      if (!isPs) {
        if (!trainFailFlag) {
          reportSuccess
        }
        val modelTrainEndTime = System.currentTimeMillis()

        val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
        try {
          //模型保存到hdfs上
          if (!keepVersion) {
            HDFSOperator.deleteDir(modelHDFSPath)
          }
          if (new File(tempModelLocalPath).exists()) {
            HDFSOperator.copyToHDFS(tempModelLocalPath, modelHDFSPath, true, false)
          } else {
            if (new File(checkpointDir).exists()) {
              HDFSOperator.copyToHDFS(checkpointDir, modelHDFSPath, true, false)
            }
          }

        } catch {
          case e: Exception =>
            reportFail
            trainFailFlag = true
        } finally {
          // delete local model
          FileUtils.deleteDirectory(new File(tempModelLocalPath))
          // delete local data
          FileUtils.deleteDirectory(new File(tempDataLocalPathWithAlgSuffix))
          FileUtils.deleteDirectory(new File(checkpointDir))
          socket.close()
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
    tfDriver.close()
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is support yet")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is support yet")
  }

}
