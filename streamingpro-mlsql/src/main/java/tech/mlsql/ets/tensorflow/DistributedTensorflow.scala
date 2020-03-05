package tech.mlsql.ets.tensorflow

import java.io.File
import java.util.concurrent.atomic.AtomicReference

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.{TaskCompletionListener, TaskContextUtil}
import org.apache.spark.{MLSQLSparkUtils, TaskContext}
import os.SubProcess
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, SQLPythonAlgParams}
import streaming.dsl.mmlib.algs.python.{MLProject, PythonAlgExecCommand}
import streaming.dsl.mmlib.algs.{Functions, SQLPythonFunc}
import tech.mlsql.arrow.python.PythonWorkerFactory
import tech.mlsql.arrow.python.runner.{PythonConf, PythonProjectRunner}
import tech.mlsql.common.utils.cluster.ml._
import tech.mlsql.common.utils.hdfs.HDFSOperator
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.network.NetUtils
import tech.mlsql.ets.ml.cluster._
import tech.mlsql.log.WriteLog

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

    val chiefIndex = fitParam.getOrElse("chiefIndex", "0").toInt

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
      override def host: String = if (MLSQLSparkUtils.rpcEnv().address == null) NetUtils.getHost
      else MLSQLSparkUtils.rpcEnv().address.host
    }

    def configureLogConf() = {
      val context = ScriptSQLExec.context()
      val conf = context.execListener.sparkSession.sqlContext.getAllConfs
      conf.filter(f => f._1.startsWith("spark.mlsql.log.driver")) ++
        Map(
          PythonWorkerFactory.Tool.REDIRECT_IMPL -> "tech.mlsql.log.RedirectStreamsToSocketServer",
          ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER) -> context.owner,
          "groupId" -> context.groupId
        )
    }

    val logConf = configureLogConf()

    val tempSocketServerHost = mlDriver._host
    val tempSocketServerPort = mlDriver._port

    val pythonProjectPath = pythonProject.filePath
    val projectName = pythonProject.projectName
    val projectTargetFileName = pythonProject.fileName
    val projectType = pythonProject.scriptType

    val envCommand = params.get(ScalaMethodMacros.str(PythonConf.PYTHON_ENV)).getOrElse(":")
    val paramCommand = params.get("PYTHON_PARAMETERS").getOrElse("")

    val command = new PythonAlgExecCommand(pythonProject, None, None, Map()).
      generateCommand(MLProject.train_command, Option(envCommand))

    def startPs = {
      ScriptSQLExec.setContext(context)
      val fitParamRDD = df.sparkSession.sparkContext.parallelize((0 until psNum).map(i => i), psNum)
      fitParamRDD.map { case algIndex =>
        val tfContext = new TFContext(DriverHost(tempSocketServerHost, tempSocketServerPort), CurrentRole("ps", algIndex))
        tfContext.assertCommand

        val hostPort = PortManager.preTaken
        val port = PortManager.getPort(hostPort)

        val workerProxy: MLWorkerProxy = tfContext.workerProxy

        workerProxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port,
          tfContext.currentRole.jobName,
          tfContext.currentRole.taskIndex,
          tfContext.isPs))

        workerProxy.waitOthers(psTargetSize + workerTargetSize)

        PortManager.releasePreTaken(hostPort)

        val paramMap = fitParam ++ Map(
          "clusterSpec" -> tfContext.createClusterSpec,
          "roleSpec" -> tfContext.createRoleSpec
        )

        //val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${projectTargetFileName} ps ${tfContext.currentRole.taskIndex}")


        val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(projectName)

        val context = TaskContext.get()

        // notice: I still get no way how to destroy the ps python worker
        // when spark existed unexpectedly.
        // we have handle the situation eg. task is cancel or
        val processRef = new AtomicReference[SubProcess]()
        val flag = new AtomicReference[String]("normal")

        TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            if (context.isInterrupted()) {
              if (processRef.get() != null) {
                tfContext.killPython(processRef.get.wrapped)
              }
            }
          }
        })

        val pythonPSThread = new Thread(new Runnable {
          override def run(): Unit = {
            TaskContextUtil.setContext(context)
            try {
              LocalDirectoryManager.downloadProject(taskDirectory, Option(pythonProjectPath), projectType)
              val runner = new PythonProjectRunner(taskDirectory, Map())
              val res = runner.run(command, paramMap ++ logConf)
              processRef.set(runner.getPythonProcess.get)
              WriteLog.write(res,paramMap ++ logConf)
            } catch {
              case e: Exception =>
                if (flag.get() != "kill-python") {
                  if (processRef.get() != null) {
                    tfContext.reportFails
                    tfContext.killPython(processRef.get().wrapped)
                    tfContext.close
                    throw e
                  }
                }
            }
          }
        })

        pythonPSThread.setDaemon(true)
        pythonPSThread.start()

        tfContext.waitDoneOrFail
        tfContext.reportSuccess
        pythonPSThread.interrupt()
        if (processRef.get() != null) {
          val temp = processRef.get()
          flag.set("kill-python")
          tfContext.killPython(processRef.get().wrapped)
        }
        tfContext.close
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

      val tfContext = new TFContext(DriverHost(tempSocketServerHost, tempSocketServerPort), CurrentRole("worker", algIndex))
      tfContext.assertCommand

      val hostPort = PortManager.preTaken
      val port = PortManager.getPort(hostPort)
      val workerProxy = tfContext.workerProxy

      workerProxy.reportToMaster(ReportToMasterRequest(MLSQLSparkUtils.rpcEnv().address.host, port,
        tfContext.currentRole.jobName,
        tfContext.currentRole.taskIndex,
        tfContext.isPs))

      workerProxy.waitOthers(psTargetSize + workerTargetSize)

      PortManager.releasePreTaken(hostPort)


      val paramMap = fitParam ++ Map(
        "tempModelLocalPath" -> tempModelLocalPath,
        "tempDataLocalPath" -> dataFilePath,
        "clusterSpec" -> tfContext.createClusterSpec,
        "roleSpec" -> tfContext.createRoleSpec
      )

      //val command = Seq("bash", "-c", envCommand + s" &&  python ${paramCommand} ${projectTargetFileName} worker ${tfContext.currentRole.taskIndex}")

      val taskDirectory = LocalDirectoryManager.setUpTaskDirectory(projectName)
      LocalDirectoryManager.downloadProject(taskDirectory, Option(pythonProjectPath), projectType)

      val processRef = new AtomicReference[SubProcess]()

      TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          if (context.isInterrupted()) {
            if (processRef.get() != null) {
              tfContext.killPython(processRef.get.wrapped)
            }
          }
        }
      })

      try {
        val runner = new PythonProjectRunner(taskDirectory, Map())
        val res = runner.run(command, paramMap ++ logConf)
        processRef.set(runner.getPythonProcess.get)
        WriteLog.write(res,paramMap ++ logConf)
        logInfo(s"report worker ${tfContext.currentRole.taskIndex} success")
        tfContext.reportSuccess
      } catch {
        case e: Exception =>
          logError(s"Fail to run ${tfContext.currentRole.jobName}:${tfContext.currentRole.taskIndex} ", e)
          tfContext.reportFails
          tfContext.close
          throw e
      }

      val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
      try {
        //模型保存到hdfs上
        if (!keepVersion) {
          HDFSOperator.deleteDir(modelHDFSPath)
        }
        if (new File(tempModelLocalPath).exists() && chiefIndex == tfContext.currentRole.taskIndex) {
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
      tfContext.close
      Iterator()
    }.count()
    pSThread.join()

    val tfContext = new TFContext(DriverHost(tempSocketServerHost, tempSocketServerPort), CurrentRole("worker", -1))
    val res = tfContext.workerProxy.fetchClusterSpec()

    import df.sparkSession.implicits._
    val newDF = df.sparkSession.createDataset(res.workers ++ res.parameterServers).toDF()
    tfContext.close
    mlDriver.close()
    newDF
  }

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is support yet")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    throw new RuntimeException("register is support yet")
  }

}
