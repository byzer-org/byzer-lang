package streaming.dsl.mmlib.algs

import java.io.File
import java.util
import java.util.{ArrayList, UUID}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.ps.cluster.Message
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
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

    // save data to hdfs and broadcast validate table
    val dataManager = new DataManager(df, path, params)
    val enableDataLocal = dataManager.enableDataLocal
    val dataHDFSPath = dataManager.saveDataToHDFS
    val rowsBr = dataManager.broadCastValidateTable

    var stopFlagNum = -1
    if (!enableDataLocal) {
      require(kafkaParam.size > 0, "We detect that you do not set enableDataLocal true, " +
        "in this situation, kafkaParam should be configured")
      val (_kafkaParam, _newRDD) = writeKafka(df, path, params)
      stopFlagNum = _newRDD.getNumPartitions
      kafkaParam = _kafkaParam
    }

    val systemParam = mapParams("systemParam", params)
    var fitParam = arrayParamsWithIndex("fitParam", params)

    if (fitParam.size == 0) {
      fitParam = Array(0 -> Map[String, String]())
    }
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
    val projectType = pythonProject.get.scriptType

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

      if (!kafkaParam.isEmpty) {
        val kafkaP = kafkaParam + ("group_id" -> (kafkaParam("group_id") + "_" + algIndex))
        paramMap.put("kafkaParam", kafkaP.asJava)
      }

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
          if (projectType == MLFlow) {
            downloadPythonProject(taskDirectory, pythonProjectPath)
          } else {
            downloadPythonProject(taskDirectory + "/" + pythonProjectPath.get.split("/").last, pythonProjectPath)
          }


        case None => // this will not happen, cause even script is a project contains only one python script file.
      }


      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig)).
        generateCommand(MLProject.train_command)

      val modelTrainStartTime = System.currentTimeMillis()

      var score = 0.0
      var trainFailFlag = false
      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
        envVars = envs,
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

        def filterScore(str: String) = {
          if (str != null && str.startsWith("mlsql_validation_score:")) {
            str.split(":").last.toDouble
          } else 0d
        }

        val scores = res.map(f => filterScore(f)).toSeq
        score = if (scores.size > 0) scores.head else 0d
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

    if (!SQLPythonAlg.isAPIService()) {
      throw new RuntimeException(
        s"""
           |Register statement in PythonAlg module only support in API deploy mode.
         """.stripMargin)
    }
    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    val localPathConfig = LocalPathConfig.buildFromParams(_path)

    val taskDirectory = localPathConfig.localRunPath + "/" + modelMeta.pythonScript.projectName

    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)


    modelMeta.pythonScript.scriptType match {
      case MLFlow =>
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

    modelMeta.copy(resources = selectedFitParam + ("resource" -> resourceParams.asJava), taskDirectory = Option(taskDirectory))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    new APIPredict().predict(sparkSession, _model.asInstanceOf[ModelMeta], name, params)
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
