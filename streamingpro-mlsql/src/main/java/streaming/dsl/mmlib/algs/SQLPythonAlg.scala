package streaming.dsl.mmlib.algs

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.{ArrayList, UUID}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{APIDeployPythonRunnerEnv, TaskContext}
import org.apache.spark.api.python.WowPythonRunner
import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.util.ObjPickle._
import org.apache.spark.util.VectorSerDer._
import org.apache.spark.util._
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.SQLPythonFunc._

import scala.collection.JavaConverters._
import streaming.common.HDFSOperator
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, SQLPythonAlgParams}
import streaming.dsl.mmlib.algs.python._
import streaming.common.ScalaMethodMacros._


/**
  * Created by allwefantasy on 5/2/2018.
  * This Module support training or predicting with user-defined python script
  */
class SQLPythonAlg(override val uid: String) extends SQLAlg with Functions with SQLPythonAlgParams {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "false").toBoolean

    var kafkaParam = mapParams("kafkaParam", params)

    require(kafkaParam.size > 0, "kafkaParam should be configured")

    // save data to hdfs and broadcast validate table
    val dataManager = new DataManager(df, path, params)
    val enableDataLocal = dataManager.enableDataLocal
    val dataHDFSPath = dataManager.saveDataToHDFS
    val rowsBr = dataManager.broadCastValidateTable

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


    //configuration about project and env
    val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
    val envs = EnvConfig.buildFromSystemParam(systemParam)


    // find python project
    val pythonProject = PythonAlgProject.loadProject(params, df.sparkSession)


    incrementVersion(path, keepVersion)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val pythonProjectPath = Option(pythonProject.get.filePath)

    val projectName = pythonProject.get.projectName

    val wowRDD = fitParamRDD.map { paramAndIndex =>

      ScriptSQLExec.setContext(mlsqlContext)


      val f = paramAndIndex._2
      val algIndex = paramAndIndex._1

      val localPathConfig = LocalPathConfig.buildFromParams(path)
      var tempDataLocalPathWithAlgSuffix = localPathConfig.localDataPath

      if (enableDataLocal) {
        tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
        val msg = s"dataLocalFormat enabled ,system will generate data in ${tempDataLocalPathWithAlgSuffix} "
        logInfo(format(msg))
        recordSingleLineLog(kafkaParam, msg)
        HDFSOperator.copyToLocalFile(tempLocalPath = tempDataLocalPathWithAlgSuffix, path = dataHDFSPath, true)
      }

      val paramMap = new util.HashMap[String, Object]()
      var item = f.asJava
      if (!f.contains("modelPath")) {
        item = (f + ("modelPath" -> path)).asJava
      }

      val resourceParams = new ResourceManager(f).loadResourceInTrain

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName
      val tempModelLocalPath = s"${localPathConfig.localModelPath}/${algIndex}"
      //FileUtils.forceMkdir(tempModelLocalPath)

      paramMap.put("fitParam", item)

      val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
      paramMap.put("kafkaParam", kafkaP.asJava)

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.stopFlagNum) -> stopFlagNum,
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> tempModelLocalPath,
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> tempDataLocalPathWithAlgSuffix,
        str[RunPythonConfig.InternalSystemParam](_.resource) -> resourceParams.asJava
      )

      paramMap.put(RunPythonConfig.internalSystemParam, internalSystemParam.asJava)
      paramMap.put(RunPythonConfig.systemParam, systemParam.asJava)

      pythonProjectPath match {
        case Some(_) =>
          downloadPythonProject(taskDirectory, pythonProjectPath)

        case None => // just normal script
      }


      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig)).
        generateCommand(MLProject.train_command)

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var trainFailFlag = false
      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
        envVars = envs,
        recordLog = SQLPythonFunc.recordAnyLog(kafkaParam),
        logCallback = (msg) => {
          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
          logInfo(format(msg))
        }
      )
      try {

        val res = runner.run(
          command = command,
          params = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonProject.get.fileContent,
          scriptName = pythonProject.get.fileName,
          validateData = rowsBr.value
        )

        score = recordUserLog(algIndex, pythonProject.get, kafkaParam, res, logCallback = (msg) => {
          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
          logInfo(format(msg))
        })
      } catch {
        case e: Exception =>
          logError(format_cause(e))
          e.printStackTrace()
          trainFailFlag = true
      }

      val modelTrainEndTime = System.currentTimeMillis()

      val modelHDFSPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + algIndex
      try {
        //copy model to HDFS
        val fs = FileSystem.get(new Configuration())
        if (!keepVersion) {
          fs.delete(new Path(modelHDFSPath), true)
        }
        fs.copyFromLocalFile(new Path(tempModelLocalPath),
          new Path(modelHDFSPath))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          trainFailFlag = true
      } finally {
        // delete local model
        FileUtils.deleteDirectory(new File(tempModelLocalPath))
        // delete local data
        FileUtils.deleteDirectory(new File(tempDataLocalPathWithAlgSuffix))
      }
      val status = if (trainFailFlag) "fail" else "success"
      Row.fromSeq(Seq(modelHDFSPath, algIndex, pythonProject.get.fileName, score, status, modelTrainStartTime, modelTrainEndTime, f))
    }

    df.sparkSession.createDataFrame(wowRDD, PythonTrainingResultSchema.algSchema).write.mode(SaveMode.Overwrite).parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")

    val tempRDD = df.sparkSession.sparkContext.parallelize(Seq(Seq(
      Map(
        str[PythonConfig](_.pythonPath) -> pythonConfig.pythonPath,
        str[PythonConfig](_.pythonVer) -> pythonConfig.pythonVer
      ), params)), 1).map { f =>
      Row.fromSeq(f)
    }
    df.sparkSession.createDataFrame(tempRDD, PythonTrainingResultSchema.trainParamsSchema).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/1")

    df.sparkSession.read.parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }

  /*

     when you use register statement ,this means you are in API predict mode.

     Since python worker in predict mode is long-run process, we can not use conda to
     find the best env for every project because they share the same python workers.

     In API predict mode,  it's recommended that only deploy one python project.
     When you register model, the system will just distribute the python project and you should add the project
     in your predict script manually e.g. `sys.path.insert(0,mlsql.internal_system_param["resource"]["mlFlowProjectPath"])
     and we will not create suitable env for you for now.

   */
  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    val localPathConfig = LocalPathConfig.buildFromParams(_path)

    val taskDirectory = localPathConfig.localRunPath + "/" + modelMeta.pythonScript.projectName

    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)


    modelMeta.pythonScript.scriptType match {
      case MLFlow =>
        if (SQLPythonAlg.isAPIService()) {
          logWarning(format(
            s"""
               |Detect that you are registering MLFlow project but it's not in API mode.
               |In this situation, we can not use conda to create suitable python environment.
               |Please use batch predict.
               |
               |More detail:
               |
               |load modelExample.`PythonAlg` as output;
               |
               |""".stripMargin))
        }

        distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
          resourceParams += ("mlFlowProjectPath" -> path)
        })

      case _ => None
    }
    val pythonProjectPath = params.get("pythonProjectPath")

    if (pythonProjectPath.isDefined) {
      distributePythonProject(sparkSession, taskDirectory, Option(modelMeta.pythonScript.filePath)).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }

    modelMeta.copy(resources = selectedFitParam + ("resource" -> resourceParams.asJava))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val modelMeta = _model.asInstanceOf[ModelMeta]
    val models = sparkSession.sparkContext.broadcast(modelMeta.modelEntityPaths)
    val trainParams = modelMeta.trainParams
    val systemParam = mapParams("systemParam", trainParams)


    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
    val envs = EnvConfig.buildFromSystemParam(systemParam)


    val userPredictScript = findPythonPredictScript(sparkSession, params, "")

    val maps = new util.HashMap[String, java.util.Map[String, _]]()
    val item = new util.HashMap[String, String]()
    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
    maps.put("systemParam", item)
    maps.put("internalSystemParam", modelMeta.resources.asJava)

    val kafkaParam = mapParams("kafkaParam", trainParams)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val enableErrorMsgToKafka = params.getOrElse("enableErrorMsgToKafka", "false").toBoolean
    val kafkaParam2 = if (enableErrorMsgToKafka) kafkaParam else Map[String, String]()

    val recordLog = SQLPythonFunc.recordAnyLog(kafkaParam2, logCallback = (msg) => {
      ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
      logInfo(format(msg))
    })
    val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)
    val enableCopyTrainParamsToPython = params.getOrElse("enableCopyTrainParamsToPython", "false").toBoolean

    val pythonRunner = new PythonProjectExecuteRunner(taskDirectory = taskDirectory, envVars = envs, recordLog = recordLog)

    /*
      Run python script in driver so we can get function then broadcast it to all
      python worker.
      Make sure you use `sys.path.insert(0,mlsql.internal_system_param["resource"]["mlFlowProjectPath"])`
      if you run it in project.
     */
    val res = pythonRunner.run(
      Seq(pythonConfig.pythonPath, userPredictScript.fileName),
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      userPredictScript.fileContent,
      userPredictScript.fileName
    )

    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))

    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap

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

      val iter = WowPythonRunner.run(
        pythonConfig.pythonPath, pythonConfig.pythonVer, command, v_ser3, TaskContext.get().partitionId(), Array(), runtimeParams, recordLog
      )
      val a = iter.next()
      val predictValue = VectorSerDer.deser_vector(unpickle(a).asInstanceOf[java.util.ArrayList[Object]].get(0))
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


  override def batchPredict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val bp = new BatchPredict()
    bp.predict(df, _path, params)
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new SQLPythonAlg()
    })
  }

  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = super.explainModel(sparkSession, path, params)

  override def skipPathPrefix: Boolean = false

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(MarkDownDoc,
    s"""
       |todo
     """.stripMargin)

  override def codeExample: Code = Code(SQLCode,
    s"""
       |todo
     """.stripMargin)

  override def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_2_x, Core_2_3_x)
  }

  def distributePythonProject(spark: SparkSession, localProjectDirectory: String, pythonProjectPath: Option[String]): Option[String] = {


    if (pythonProjectPath.isDefined) {
      logInfo(format(s"system load python project into directory: [ ${
        localProjectDirectory
      } ]."))
      distributeResource(spark, pythonProjectPath.get, localProjectDirectory)
      logInfo(format("python project loaded!"))
      Some(localProjectDirectory)
    } else {
      None
    }
  }

  def downloadPythonProject(localProjectDirectory: String, pythonProjectPath: Option[String]) = {

    if (pythonProjectPath.isDefined) {
      logInfo(format(s"system load python project into directory: [ ${
        localProjectDirectory
      } ]."))
      HDFSOperator.copyToLocalFile(localProjectDirectory, pythonProjectPath.get, true)
      logInfo(format("python project loaded!"))
      Some(localProjectDirectory)
    }
    else None

  }
}

object SQLPythonAlg {
  def createNewFeatures(list: List[Row], inputCol: String): Matrix = {
    val numRows = list.size
    val numCols = list.head.getAs[Vector](inputCol).size
    val values = new ArrayList[Double](numCols * numRows)

    val vectorArray = list.map(r => {
      r.getAs[Vector](inputCol).toArray
    })
    for (i <- (0 until numCols)) {
      for (j <- (0 until numRows)) {
        values.add(vectorArray(j)(i))
      }
    }
    Matrices.dense(numRows, numCols, values.asScala.toArray).toSparse
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

  def distributeResource(spark: SparkSession, path: String, tempLocalPath: String) = {
    if (spark.sparkContext.isLocal) {
      val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].localSchedulerBackend
      psDriverBackend.localEndpoint.askSync[Boolean](Message.CopyModelToLocal(path, tempLocalPath))
    } else {
      val psDriverBackend = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].psDriverBackend
      psDriverBackend.psDriverRpcEndpointRef.askSync[Boolean](Message.CopyModelToLocal(path, tempLocalPath))
    }
  }

  def isAPIService() = {
    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap
    runtimeParams.getOrElse("streaming.deploy.rest.api", "false").toString.toBoolean
  }
}
