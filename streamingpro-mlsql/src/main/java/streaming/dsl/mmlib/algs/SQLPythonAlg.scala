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
import org.apache.spark.ml.feature.PythonBatchPredictDataSchema
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

import scala.collection.mutable

/**
  * Created by allwefantasy on 5/2/2018.
  * This Module support training or predicting with user-defined python script
  */
class SQLPythonAlg(override val uid: String) extends SQLAlg with Functions with SQLPythonAlgParams {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "false").toBoolean

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


    val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
    val envs = EnvConfig.buildFromSystemParam(systemParam)

    var dataHDFSPath = ""

    // persist training data to HDFS
    if (enableDataLocal) {
      val dataLocalizeConfig = DataLocalizeConfig.buildFromParams(params)
      dataHDFSPath = SQLPythonFunc.getAlgTmpPath(path) + "/data"

      val newDF = if (dataLocalizeConfig.dataLocalFileNum > -1) {
        df.repartition(dataLocalizeConfig.dataLocalFileNum)
      } else df
      newDF.write.format(dataLocalizeConfig.dataLocalFormat).mode(SaveMode.Overwrite).save(dataHDFSPath)
    }

    val pythonScript = loadUserDefinePythonScript(params, df.sparkSession)

    val schema = df.schema
    var rows = Array[Array[Byte]]()
    //目前我们只支持同一个测试集
    if (params.contains("validateTable") || params.contains("evaluateTable")) {
      val validateTable = params.getOrElse("validateTable", params.getOrElse("evaluateTable", ""))
      rows = df.sparkSession.table(validateTable).rdd.mapPartitions { iter =>
        ObjPickle.pickle(iter, schema)
      }.collect()
    }
    val rowsBr = df.sparkSession.sparkContext.broadcast(rows)

    incrementVersion(path, keepVersion)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    distributePythonProject(df.sparkSession, path, params)

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

      val taskDirectory = localPathConfig.localRunPath
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


      val command = new PythonAlgExecCommand(pythonScript.get, Option(mlflowConfig), Option(pythonConfig)).generateCommand

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
        val projectName = pythonScript.get.scriptType match {
          case MLFlow => Some(pythonScript.get.fileContent)
          case _ => None
        }
        val res = runner.run(
          command = command,
          iter = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonScript.get.fileContent,
          scriptName = pythonScript.get.fileName,
          projectName = projectName,
          validateData = rowsBr.value
        )

        score = recordUserLog(algIndex, pythonScript.get, kafkaParam, res, logCallback = (msg) => {
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
        //模型保存到hdfs上
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
      Row.fromSeq(Seq(modelHDFSPath, algIndex, pythonScript.get.fileName, score, status, modelTrainStartTime, modelTrainEndTime, f))
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

  override def load(sparkSession: SparkSession, _path: String, params: Map[String, String]): Any = {

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel

    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(sparkSession, modelMeta)
    val loadPythonProject = modelMeta.trainParams.contains("pythonProjectPath")

    if (loadPythonProject) {
      distributePythonProject(sparkSession, _path, modelMeta.trainParams).foreach(path => {
        resourceParams += ("pythonProjectPath" -> path)
      })
    }

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
        distributePythonProject(sparkSession, modelMeta.pythonScript.filePath, modelMeta.trainParams).foreach(path => {
          resourceParams += ("mlFlowProjectPath" -> path)
        })
      case _ => None
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

    val projectName = modelMeta.pythonScript.scriptType match {
      case MLFlow =>
        Some(modelMeta.pythonScript.fileContent)
      case _ => None
    }

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
      userPredictScript.fileName,
      projectName
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


  def batchPredictForAPI(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val sparkSession = df.sparkSession

    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel

    val batchSize = params.getOrElse("batchSize", "10").toInt
    val inputCol = params.getOrElse("inputCol", "")

    require(inputCol != null && inputCol != "", s"inputCol in ${getClass} module should be configured!")

    val batchPredictFun = params.getOrElse("predictFun", UUID.randomUUID().toString.replaceAll("-", ""))
    val predictLabelColumnName = params.getOrElse("predictCol", "predict_label")
    val predictTableName = params.getOrElse("predictTable", "")

    require(
      predictTableName != null && predictTableName != "",
      s"predictTable in ${getClass} module should be configured!"
    )

    val schema = PythonBatchPredictDataSchema.newSchema(df)

    val rdd = df.rdd.mapPartitions(it => {
      var list = List.empty[List[Row]]
      var tmpList = List.empty[Row]
      var batchCount = 0
      while (it.hasNext) {
        val e = it.next()
        if (batchCount == batchSize) {
          list +:= tmpList
          batchCount = 0
          tmpList = List.empty[Row]
        } else {
          tmpList +:= e
          batchCount += 1
        }
      }
      if (batchCount != batchSize) {
        list +:= tmpList
      }
      list.map(x => {
        Row.fromSeq(Seq(x, SQLPythonAlg.createNewFeatures(x, inputCol)))
      }).iterator
    })

    val systemParam = mapParams("systemParam", modelMeta.trainParams)
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val pythonVer = systemParam.getOrElse("pythonVer", "2.7")
    val kafkaParam = mapParams("kafkaParam", modelMeta.trainParams)

    // load python script
    val userPythonScript = SQLPythonFunc.findPythonPredictScript(sparkSession, params, "")

    val maps = new java.util.HashMap[String, java.util.Map[String, _]]()
    val item = new java.util.HashMap[String, String]()
    item.put("funcPath", "/tmp/" + System.currentTimeMillis())
    maps.put("systemParam", item)
    maps.put("internalSystemParam", modelMeta.resources.asJava)

    val taskDirectory = SQLPythonFunc.getLocalRunPath(UUID.randomUUID().toString)

    val res = ExternalCommandRunner.run(taskDirectory, Seq(pythonPath, userPythonScript.fileName),
      maps,
      MapType(StringType, MapType(StringType, StringType)),
      userPythonScript.fileContent,
      userPythonScript.fileName, modelPath = null, recordLog = SQLPythonFunc.recordAnyLog(kafkaParam)
    )
    res.foreach(f => f)
    val command = Files.readAllBytes(Paths.get(item.get("funcPath")))
    val runtimeParams = PlatformManager.getRuntime.params.asScala.toMap

    // registe batch predict python function

    val recordLog = SQLPythonFunc.recordAnyLog(Map[String, String]())
    val models = sparkSession.sparkContext.broadcast(modelMeta.modelEntityPaths)
    val f = (m: Matrix, modelPath: String) => {
      val modelRow = InternalRow.fromSeq(Seq(SQLPythonFunc.getLocalTempModelPath(modelPath)))
      val trainParamsRow = InternalRow.fromSeq(Seq(ArrayBasedMapData(params)))
      val v_ser = ObjPickle.pickleInternalRow(Seq(MatrixSerDer.serialize(m)).toIterator, MatrixSerDer.matrixSchema())
      val v_ser2 = ObjPickle.pickleInternalRow(Seq(modelRow).toIterator, StructType(Seq(StructField("modelPath", StringType))))
      val v_ser3 = v_ser ++ v_ser2

      if (TaskContext.get() == null) {
        APIDeployPythonRunnerEnv.setTaskContext(APIDeployPythonRunnerEnv.createTaskContext())
      }
      val iter = WowPythonRunner.run(
        pythonPath, pythonVer, command, v_ser3, TaskContext.get().partitionId(), Array(), runtimeParams, recordLog
      )
      val a = iter.next()
      val predictValue = MatrixSerDer.deserialize(ObjPickle.unpickle(a).asInstanceOf[java.util.ArrayList[Object]].get(0))
      predictValue
    }

    val f2 = (m: Matrix) => {
      models.value.map { modelPath =>
        f(m, modelPath)
      }.head
    }

    val func = UserDefinedFunction(f2, MatrixType, Some(Seq(MatrixType)))
    sparkSession.udf.register(batchPredictFun, func)

    // temp batch predict column name
    val tmpPredColName = UUID.randomUUID().toString.replaceAll("-", "")
    val pdf = sparkSession.createDataFrame(rdd, schema)
      .selectExpr(s"${batchPredictFun}(newFeature) as ${tmpPredColName}", "originalData")

    val prdd = pdf.rdd.mapPartitions(it => {
      var list = List.empty[Row]
      while (it.hasNext) {
        val e = it.next()
        val originalData = e.getAs[mutable.WrappedArray[Row]]("originalData")
        val newFeature = e.getAs[Matrix](tmpPredColName).rowIter.toList
        val size = originalData.size
        (0 until size).map(index => {
          val od = originalData(index)
          val pd = newFeature(index)
          list +:= Row.fromSeq(od.toSeq ++ Seq(pd))
        })
      }
      list.iterator
    })
    val pschema = df.schema.add(predictLabelColumnName, VectorType)
    val newdf = sparkSession.createDataFrame(prdd, pschema)
    newdf.createOrReplaceTempView(predictTableName)
    newdf
  }

  def batchPredictForBatch(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    //    val sparkSession = df.sparkSession
    //    import sparkSession.implicits._
    //    val modelMetaManager = new ModelMetaManager(sparkSession, _path, params)
    //    val modelMeta = modelMetaManager.loadMetaAndModel
    //
    //    modelMeta.pythonScript.scriptType match {
    //      case MLFlow =>
    //        distributePythonProject(sparkSession, modelMeta.pythonScript.filePath, modelMeta.trainParams)
    //      case _ => throw new RuntimeException("When predictMode is batch only support MLFlow project.")
    //    }
    //
    //    val projectName = modelMeta.pythonScript.fileContent
    //
    //
    //    val systemParam = mapParams("systemParam", modelMeta.trainParams)
    //    val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
    //    val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
    //    val envs = EnvConfig.buildFromSystemParam(systemParam)
    //    val kafkaParam = mapParams("kafkaParam", modelMeta.trainParams)
    //    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()
    //
    //    val pythonPredictScript = SQLPythonFunc.findPythonPredictScript(sparkSession, params, "")
    //
    //
    //    df.mapPartitions { iter =>
    //      ScriptSQLExec.setContext(mlsqlContext)
    //      // taskDirectory should be uniq, so we should place build function here
    //      val localPathConfig = LocalPathConfig.buildFromParams(_path)
    //      val taskDirectory = localPathConfig.localRunPath
    //
    //      val command = new PythonAlgExecCommand(modelMeta.pythonScript, Option(mlflowConfig), Option(pythonConfig)).generateCommand
    //
    //      val runner = new PythonProjectExecuteRunner(
    //        taskDirectory = taskDirectory,
    //        modelPath = _path,
    //        envVars = envs,
    //        recordLog = SQLPythonFunc.recordAnyLog(kafkaParam),
    //        logCallback = (msg) => {
    //          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
    //          logInfo(format(msg))
    //        }
    //      )
    //      try {
    //        val res = runner.run(
    //          command = command,
    //          iter = paramMap,
    //          schema = MapType(StringType, MapType(StringType, StringType)),
    //          scriptContent = pythonPredictScript.fileContent,
    //          scriptName = pythonPredictScript.fileName,
    //          projectName = modelMeta.pythonScript.fileContent,
    //          validateData = rowsBr.value
    //        )
    //
    //        score = recordUserLog(algIndex, pythonScript.get, kafkaParam, res, logCallback = (msg) => {
    //          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
    //          logInfo(format(msg))
    //        })
    //      } catch {
    //        case e: Exception =>
    //          logError(format_cause(e))
    //          e.printStackTrace()
    //          trainFailFlag = true
    //      }
    //      iter
    //    }

    null

  }


  override def batchPredict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val predictMode = params.getOrElse("predictMode", "batch")
    predictMode match {
      case "batch" => batchPredictForBatch(df, _path, params)
      case "api" => batchPredictForAPI(df, _path, params)
    }
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

  def distributePythonProject(spark: SparkSession, path: String, params: Map[String, String]): Option[String] = {
    val userPythonScript = loadUserDefinePythonScript(params, spark)
    // load python project
    val pythonProjectPath = userPythonScript.get.scriptType match {
      case MLFlow => Some(userPythonScript.get.filePath)
      case _ => params.get("pythonProjectPath")
    }

    if (pythonProjectPath.isDefined) {
      val tempPythonProjectLocalPath = SQLPythonFunc.getLocalRunPath(path) + "/" + userPythonScript.get.filePath.split("/").last
      logInfo(format(s"system load python project into directory: [ ${
        tempPythonProjectLocalPath
      } ]."))
      distributeResource(spark, pythonProjectPath.get, tempPythonProjectLocalPath)
      logInfo(format("python project loaded!"))
      Some(tempPythonProjectLocalPath)
    } else {
      None
    }
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
