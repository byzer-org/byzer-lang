package tech.mlsql.ets.tensorflow

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.TaskContextUtil
import org.apache.spark.{MLSQLSparkUtils, TaskContext}
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import streaming.dsl.mmlib.algs.param.{BaseParams, SQLPythonAlgParams}
import streaming.dsl.mmlib.algs.{Functions, SQLPythonFunc}
import tech.mlsql.arrow.python.runner.{PythonConf, PythonProjectRunner}
import tech.mlsql.common.utils.cluster.ml.{JobStatusRequest, MLDriver, MLWorkerProxy, ReportToMasterRequest}
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.ets.ml.cluster.{DataManager, LocalDirectoryManager, LocalPathRes, PortManager}

import scala.collection.JavaConverters._

class DistributedTensorflow(override val uid: String) extends SQLAlg with SQLPythonAlgParams with Functions {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, _params: Map[String, String]): DataFrame = {


    val (pythonProject, params) = setupProject(df, path, _params)

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    val fitParam = arrayParamsWithIndex("fitParam", params).headOption match {
      case items =>
        require(items.size == 1, "fitParam only support one group")
        items.head._2
      case None =>
        throw new MLSQLException("fitParam only support one group")
    }


    incrementVersion(path, keepVersion)

    val workerTargetSize = df.rdd.partitions.size
    // start ps
    val psNum = fitParam.get("ps.num").getOrElse("1").toInt
    val psTargetSize = psNum
    val context = ScriptSQLExec.context()

    val resource = new MLSQLJobCollect(df.sparkSession, ScriptSQLExec.context().owner).resourceSummary(null)

    require((psTargetSize + workerTargetSize) < resource.totalCores,
      s"""
         |TF needs at lease ${psTargetSize + workerTargetSize} cores,but this MLSQL Engine
         |only have ${resource.totalCores},and the other job take ${resource.activeTasks} already.
       """.stripMargin)

    // We start a temp driver server, so the workers can connect it.
    // It will be used to coordinate the ps workers.
    val mlDriver = new MLDriver[String](new AtomicReference[String]("")) {
      override def host: String = MLSQLSparkUtils.rpcEnv().address.host
    }

    val tempSocketServerHost = mlDriver._host
    val tempSocketServerPort = mlDriver._port

    val pSThread = new Thread(new Runnable {
      override def run(): Unit = {
        ScriptSQLExec.setContext(context)
        val fitParamRDD = df.sparkSession.sparkContext.parallelize((0 until psNum).map(i => i), psNum)
        fitParamRDD.map { algIndex =>

          val jobName = "worker"
          val taskIndex = algIndex
          val roleSpec = new JSONObject()
          roleSpec.put("jobName", "worker")
          roleSpec.put("taskIndex", algIndex)

          val hostPort = PortManager.preTaken
          val port = PortManager.getPort(hostPort)
          val proxy = new MLWorkerProxy(tempSocketServerHost, tempSocketServerPort)
          proxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port, jobName, taskIndex, isPs = true))
          proxy.waitOthers(psTargetSize + workerTargetSize)
          PortManager.releasePreTaken(hostPort)

          val clusterSpec = proxy.fetchClusterSpec()
          val clusterSpecJson = new JSONObject()
          clusterSpecJson.put("worker", clusterSpec.workers.asJava)
          clusterSpecJson.put("ps", clusterSpec.parameterServers.asJava)

          val tempModelLocalPath = s"${LocalDirectoryManager.localModelPath}/${algIndex}"
          val paramMap = fitParam ++ Map(
            "tempModelLocalPath" -> tempModelLocalPath,
            "clusterSpec" -> clusterSpecJson.toString(),
            "roleSpec" -> roleSpec.toString(),
          )


          val envCommand = params.get(ScalaMethodMacros.str(PythonConf.PYTHON_ENV)).getOrElse("")
          val paramCommand = params.get("PYTHON_PARAMETERS").getOrElse("")
          val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${pythonProject.fileName}")

          val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(pythonProject.projectName)

          val pythonWorker = new AtomicReference[Process]()
          val context = TaskContext.get()

          // notice: I still get no way how to destroy the ps python worker
          // when spark existed unexpectedly.
          // we have handle the situation eg. task is cancel or
          val pythonPSThread = new Thread(new Runnable {
            override def run(): Unit = {
              TaskContextUtil.setContext(context)
              try {
                val runner = new PythonProjectRunner(taskDirectory, Map())
                val res = runner.run(command, paramMap)
                pythonWorker.set(res.asInstanceOf[ {def getWorker: Process}].getWorker)
                res.foreach(println)
              } catch {
                case e: Exception =>
                  e.printStackTrace()
                  proxy.reportFails(JobStatusRequest(jobName, taskIndex, true, false))
              }
            }
          })
          pythonPSThread.setDaemon(true)
          pythonPSThread.start()

          proxy.waitDoneOrFail(clusterSpec.workers.size, JobStatusRequest(jobName, taskIndex, false, false))

          pythonWorker.get().destroy()
          pythonPSThread.interrupt()
          proxy.close
          Iterator()
        }.count()

      }
    })

    pSThread.setDaemon(true)
    pSThread.start()


    // start workers
    val sourceSchema = df.schema

    df.rdd.mapPartitionsWithIndex { case (algIndex, iter) =>
      val LocalPathRes(dataFilePath, localPathConfig) = DataManager.setupData(iter, sourceSchema, algIndex)

      val tempModelLocalPath = s"${localPathConfig.localModelPath}/${algIndex}"

      val jobName = "worker"
      val taskIndex = algIndex
      val roleSpec = new JSONObject()
      roleSpec.put("jobName", "worker")
      roleSpec.put("taskIndex", algIndex)


      val hostPort = PortManager.preTaken
      val port = PortManager.getPort(hostPort)
      val proxy = new MLWorkerProxy(tempSocketServerHost, tempSocketServerPort)
      proxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port, jobName, taskIndex, false))
      proxy.waitOthers(workerTargetSize)

      PortManager.releasePreTaken(hostPort)
      val clusterSpec = proxy.fetchClusterSpec()
      val clusterSpecJson = new JSONObject()
      clusterSpecJson.put("worker", clusterSpec.workers.asJava)
      clusterSpecJson.put("ps", clusterSpec.parameterServers.asJava)

      val paramMap = fitParam ++ Map(
        "tempModelLocalPath" -> tempModelLocalPath,
        "tempDataLocalPath" -> dataFilePath,
        "clusterSpec" -> clusterSpecJson.toString(),
        "roleSpec" -> roleSpec.toString(),
      )

      val envCommand = params.get(ScalaMethodMacros.str(PythonConf.PYTHON_ENV)).getOrElse("")
      val paramCommand = params.get("PYTHON_PARAMETERS").getOrElse("")
      val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${pythonProject.fileName}")

      val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(pythonProject.projectName)
      val runner = new PythonProjectRunner(taskDirectory, Map())
      val res = runner.run(command, paramMap)
      res.foreach(println)

      proxy.reportSuccess(JobStatusRequest(jobName, taskIndex, true, true))

      val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
      try {
        //模型保存到hdfs上
        if (!keepVersion) {
          HDFSOperator.deleteDir(modelHDFSPath)
        }
        if (new File(tempModelLocalPath).exists()) {
          HDFSOperator.copyToHDFS(tempModelLocalPath, modelHDFSPath, true, false)
        }

      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        // delete local model
        FileUtils.deleteDirectory(new File(tempModelLocalPath))
        // delete local data
        FileUtils.deleteDirectory(new File(localPathConfig.localDataPath))
      }
      proxy.close
      Iterator()
    }
    mlDriver.close()
    emptyDataFrame()(df)

  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is support yet")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is support yet")
  }

}
