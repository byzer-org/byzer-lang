package streaming.dsl.mmlib.algs.python

import java.nio.file.{Files, Paths}
import java.util

import org.apache.spark.{APIDeployPythonRunnerEnv, TaskContext}
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.util.ObjPickle.{pickleInternalRow, unpickle}
import org.apache.spark.util.{PythonProjectExecuteRunner, VectorSerDer}
import org.apache.spark.util.VectorSerDer.{ser_vector, vector_schema}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{Functions, SQLPythonFunc}
import streaming.log.{Logging, WowLog}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class APIPredict extends Logging with WowLog with Serializable {
  def predict(sparkSession: SparkSession, modelMeta: ModelMeta, name: String, params: Map[String, String]): UserDefinedFunction = {
    val models = sparkSession.sparkContext.broadcast(modelMeta.modelEntityPaths)
    val trainParams = modelMeta.trainParams
    val systemParam = Functions.mapParams("systemParam", trainParams)


    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)


    // if pythonScriptPath is defined in predict/run, then use it otherwise find them in train params.
    val pythonProject = PythonAlgProject.getPythonScriptPath(params) match {
      case Some(p) => PythonAlgProject.loadProject(params, sparkSession)
      case None => PythonAlgProject.loadProject(modelMeta.trainParams, sparkSession)
    }

    val maps = new util.HashMap[String, java.util.Map[String, _]]()
    val item = new util.HashMap[String, String]()
    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
    maps.put("systemParam", item)
    maps.put("internalSystemParam", modelMeta.resources.asJava)

    val kafkaParam = Functions.mapParams("kafkaParam", trainParams)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val enableErrorMsgToKafka = params.getOrElse("enableErrorMsgToKafka", "false").toBoolean
    val kafkaParam2 = if (enableErrorMsgToKafka) kafkaParam else Map[String, String]()

    val recordLog = SQLPythonFunc.recordAnyLog(kafkaParam2, logCallback = (msg) => {
      ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
      logInfo(format(msg))
    })
    val taskDirectory = modelMeta.taskDirectory.get
    val enableCopyTrainParamsToPython = params.getOrElse("enableCopyTrainParamsToPython", "false").toBoolean


    val condaEnvManager = new CondaEnvManager()
    val condaEnvName = condaEnvManager.getOrCreateCondaEnv(Option(taskDirectory + s"/${MLProject.DEFAULT_CONDA_ENV_NAME}"))


    val envs = new util.HashMap[String, String]()
    EnvConfig.buildFromSystemParam(systemParam).foreach(f => envs.put(f._1, f._2))
    //    ++ Map(
    //      "PYTHONPATH" -> s"/anaconda3/envs/${condaEnvName}")

    val pythonRunner = new PythonProjectExecuteRunner(taskDirectory = taskDirectory, envVars = envs.asScala.toMap, recordLog = recordLog)

    val apiPredictCommand = new PythonAlgExecCommand(pythonProject.get, None, Option(pythonConfig)).
      generateCommand(MLProject.api_predict_command)

    /*
      Run python script in driver so we can get function then broadcast it to all
      python worker.
      Make sure you use `sys.path.insert(0,mlsql.internal_system_param["resource"]["mlFlowProjectPath"])`
      if you run it in project.
     */
    val res = pythonRunner.run(
      apiPredictCommand,
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      pythonProject.get.fileContent,
      pythonProject.get.fileName
    )

    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))

    val project = MLProject.loadProject(pythonProject.get.filePath)
    val daemonCommand = Seq("bash", "-c", project.condaEnvCommand + " && cd /tmp/__mlsql__/python && python -m daemon23")
    val workerCommand = Seq("bash", "-c", project.condaEnvCommand + " && cd /tmp/__mlsql__/python && python -m worker23")

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

      if (TaskContext.get() == null) {
        APIDeployPythonRunnerEnv.setTaskContext(APIDeployPythonRunnerEnv.createTaskContext())
      }

      val iter = WowPythonRunner.runner2(
        Option(daemonCommand), Option(workerCommand),
        command, envs,
        recordLog
      ).run(
        v_ser3,
        TaskContext.get().partitionId(),
        TaskContext.get()
      )
      val res = ArrayBuffer[Array[Byte]]()
      while (iter.hasNext) {
        res += iter.next()
      }

      val predictValue = VectorSerDer.deser_vector(unpickle(res(0)).asInstanceOf[java.util.ArrayList[Object]].get(0))
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
