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
import streaming.dsl.mmlib.algs.param.{BaseParams, SQLPythonAlgParams}
import streaming.dsl.mmlib.algs.{Functions, SQLPythonFunc}
import tech.mlsql.arrow.python.runner.{PythonConf, PythonProjectRunner}
import tech.mlsql.common.utils.cluster.ml._
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.ets.ml.cluster.{DataManager, LocalDirectoryManager, LocalPathRes, PortManager}

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


    SQLPythonFunc.incrementVersion(path, keepVersion)

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

    val pythonProjectPath = pythonProject.filePath
    val projectName = pythonProject.projectName
    val projectTargetFileName = pythonProject.fileName
    val projectType = pythonProject.scriptType

    val envCommand = params.get(ScalaMethodMacros.str(PythonConf.PYTHON_ENV)).getOrElse(":")
    val paramCommand = params.get("PYTHON_PARAMETERS").getOrElse("")

    def startPs = {
      ScriptSQLExec.setContext(context)
      val fitParamRDD = df.sparkSession.sparkContext.parallelize((0 until psNum).map(i => i), psNum)
      fitParamRDD.map { case algIndex =>
        val jobName = "worker"
        val taskIndex = algIndex
        val roleSpec = new JSONObject()
        roleSpec.put("jobName", "ps")
        roleSpec.put("taskIndex", taskIndex)

        val hostPort = PortManager.preTaken
        val port = PortManager.getPort(hostPort)
        val workerProxy: MLWorkerProxy = new MLWorkerProxy(tempSocketServerHost, tempSocketServerPort) {
          override def workerTaskName(): WorkerInfo => String = {
            (f: WorkerInfo) => s"/job:worker/task:${f.taskIndex}"
          }

          override def parameterServerTaskName(): WorkerInfo => String = {
            (f: WorkerInfo) => s"/job:ps/task:${f.taskIndex}"
          }
        }
        workerProxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port, jobName, taskIndex, isPs = true))
        workerProxy.waitOthers(psTargetSize + workerTargetSize)
        PortManager.releasePreTaken(hostPort)

        val clusterSpec = workerProxy.fetchClusterSpec()
        val clusterSpecMap = Map("worker" -> clusterSpec.workers.map(f => s"${f.host}:${f.port}"),
          "ps" -> clusterSpec.parameterServers.map(f => s"${f.host}:${f.port}"))

        val paramMap = fitParam ++ Map(
          "clusterSpec" -> JSONTool.toJsonStr(clusterSpecMap),
          "roleSpec" -> roleSpec.toString()
        )

        val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${projectTargetFileName}")

        val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(projectName)

        val pythonWorker = new AtomicReference[Process]()
        val context = TaskContext.get()

        // notice: I still get no way how to destroy the ps python worker
        // when spark existed unexpectedly.
        // we have handle the situation eg. task is cancel or
        val pythonPSThread = new Thread(new Runnable {
          override def run(): Unit = {
            TaskContextUtil.setContext(context)
            try {
              LocalDirectoryManager.downloadProject(taskDirectory, Option(pythonProjectPath), projectType)
              val runner = new PythonProjectRunner(taskDirectory, Map())
              val res = runner.run(command, paramMap)
              res.foreach(println)
            } catch {
              case e: Exception =>
                e.printStackTrace()

            }
          }
        })
        pythonPSThread.setDaemon(true)
        pythonPSThread.start()

        workerProxy.waitDoneOrFail(clusterSpec.workers.size, JobStatusRequest(jobName, taskIndex, false, false))
        workerProxy.reportSuccess(JobStatusRequest(jobName, taskIndex, true, true))
        pythonWorker.get().destroy()
        pythonPSThread.interrupt()
        workerProxy.close
        Iterator()
      }.count()
    }

    val pSThread = new Thread(new Runnable {
      override def run(): Unit = {
        startPs
      }
    })

    pSThread.setDaemon(true)
    pSThread.start()

    // start workers
    val sourceSchema = df.schema
    val sessionLocalTimeZone = df.sparkSession.sessionState.conf.sessionLocalTimeZone
    val fileFormat = params.getOrElse("fileFormat", "json")
    df.rdd.mapPartitionsWithIndex { case (algIndex, iter) =>

      val LocalPathRes(dataFilePath, localPathConfig) = DataManager.setupData(iter, sourceSchema, sessionLocalTimeZone, algIndex, fileFormat)

      val tempModelLocalPath = s"${localPathConfig.localModelPath}/${algIndex}"

      val jobName = "worker"
      val taskIndex = algIndex
      val roleSpec = new JSONObject()
      roleSpec.put("jobName", "worker")
      roleSpec.put("taskIndex", taskIndex)


      val hostPort = PortManager.preTaken
      val port = PortManager.getPort(hostPort)
      val workerProxy = new MLWorkerProxy(tempSocketServerHost, tempSocketServerPort) {
        override def workerTaskName(): WorkerInfo => String = {
          (f: WorkerInfo) => s"/job:worker/task:${f.taskIndex}"
        }

        override def parameterServerTaskName(): WorkerInfo => String = {
          (f: WorkerInfo) => s"/job:ps/task:${f.taskIndex}"
        }
      }
      workerProxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port, jobName, taskIndex, false))
      workerProxy.waitOthers(workerTargetSize + psTargetSize)

      PortManager.releasePreTaken(hostPort)
      val clusterSpec = workerProxy.fetchClusterSpec()

      val clusterSpecMap = Map("worker" -> clusterSpec.workers.map(f => s"${f.host}:${f.port}"),
        "ps" -> clusterSpec.parameterServers.map(f => s"${f.host}:${f.port}"))

      val paramMap = fitParam ++ Map(
        "tempModelLocalPath" -> tempModelLocalPath,
        "tempDataLocalPath" -> dataFilePath,
        "clusterSpec" -> JSONTool.toJsonStr(clusterSpecMap),
        "roleSpec" -> roleSpec.toString()
      )

      val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${projectTargetFileName}")

      val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(projectName)
      LocalDirectoryManager.downloadProject(taskDirectory, Option(pythonProjectPath), projectType)

      try {
        val runner = new PythonProjectRunner(taskDirectory, Map())
        val res = runner.run(command, paramMap)
        res.foreach(println)
        workerProxy.reportSuccess(JobStatusRequest(jobName, taskIndex, true, true))
      } catch {
        case e: Exception =>
          logError(s"Fail to run ${jobName}:${taskIndex} ", e)
          workerProxy.reportFails(JobStatusRequest(jobName, taskIndex, true, false))
      }

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
      workerProxy.close
      Iterator()
    }.count()
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
