package streaming.dsl.mmlib.algs.python

import java.io.{File, FileWriter}
import java.util

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.util.PythonProjectExecuteRunner
import streaming.common.HDFSOperator
import streaming.common.ScalaMethodMacros.str
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg, SQLPythonFunc}
import streaming.log.{Logging, WowLog}

import scala.collection.JavaConverters._

class BatchPredict extends Logging with WowLog with Serializable {
  def predict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val keepLocalDirectory = params.getOrElse("keepLocalDirectory", "false").toBoolean
    val modelMetaManager = new ModelMetaManager(spark, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    var (selectedFitParam, resourceParams) = new ResourceManager(params).loadResourceInRegister(spark, modelMeta)

    modelMeta.copy(resources = selectedFitParam)
    val resources = modelMeta.resources

    // if pythonScriptPath is defined in predict/run, then use it otherwise find them in train params.
    val pythonProject = PythonAlgProject.getPythonScriptPath(params) match {
      case Some(p) => PythonAlgProject.loadProject(params, df.sparkSession)
      case None => PythonAlgProject.loadProject(modelMeta.trainParams, df.sparkSession)
    }
    val projectName = pythonProject.get.projectName

    val systemParam = Functions.mapParams("systemParam", modelMeta.trainParams)

    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    val schema = df.schema
    val sessionLocalTimeZone = df.sparkSession.sessionState.conf.sessionLocalTimeZone

    val modelPath = modelMeta.modelEntityPaths.head

    val outoutFile = SQLPythonFunc.getAlgTmpPath(_path) + "/output"

    val jsonRDD = df.rdd.mapPartitionsWithIndex { (index, iter) =>
      ScriptSQLExec.setContext(mlsqlContext)
      val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
      val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
      val envs = EnvConfig.buildFromSystemParam(systemParam)

      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig), envs).
        generateCommand(MLProject.batch_predict_command)

      val localPathConfig = LocalPathConfig.buildFromParams(_path)

      val localDataFile = new File(localPathConfig.localDataPath)
      if (!localDataFile.exists()) {
        localDataFile.mkdirs()
      }

      val fileWriter = new FileWriter(new File(localPathConfig.localDataPath + s"/${index}.json"))
      try {
        WowJsonInferSchema.toJson(iter, schema, sessionLocalTimeZone, callback = (row) => {
          fileWriter.write(row + "\n")
        })
      } catch {
        case e: Exception =>
          logError(format_exception(e))
      } finally {
        fileWriter.close()
      }

      val paramMap = new util.HashMap[String, Object]()

      val localModelPath = localPathConfig.localModelPath + s"/${index}"
      HDFSOperator.copyToLocalFile(localModelPath, modelPath, true)

      val localOutputFileStr = localPathConfig.localOutputPath + s"/output-${index}.json"

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> (localPathConfig.localDataPath + s"/${index}.json"),
        str[RunPythonConfig.InternalSystemParam](_.tempOutputLocalPath) -> localOutputFileStr,
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> localModelPath
      )


      val localOutputPathFile = new File(localPathConfig.localOutputPath)
      if (!localOutputPathFile.exists()) {
        localOutputPathFile.mkdirs()
      }

      paramMap.put(RunPythonConfig.internalSystemParam, (resources ++ internalSystemParam).asJava)
      paramMap.put(RunPythonConfig.systemParam, systemParam.asJava)

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName

      SQLPythonAlg.downloadPythonProject(taskDirectory, Option(pythonProject.get.filePath))

      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
        keepLocalDirectory = keepLocalDirectory,
        envVars = envs,
        logCallback = (msg) => {
          ScriptSQLExec.setContextIfNotPresent(mlsqlContext)
          logInfo(format(msg))
        }
      )
      var trainFailFlag = false
      try {

        val res = runner.run(
          command = command,
          params = paramMap,
          schema = MapType(StringType, MapType(StringType, StringType)),
          scriptContent = pythonProject.get.fileContent,
          scriptName = pythonProject.get.fileName,
          validateData = Array()
        )
        res.foreach(f => logInfo(format(f)))

        HDFSOperator.copyToHDFS(localOutputFileStr, outoutFile, cleanTarget = true, cleanSource = false)

      } catch {
        case e: Exception =>
          logError(format_cause(e))
          e.printStackTrace()
          trainFailFlag = true
      } finally {
        FileUtils.deleteDirectory(new File(localModelPath))
        FileUtils.deleteDirectory(new File(localPathConfig.localDataPath))
        FileUtils.deleteDirectory(new File(localPathConfig.localOutputPath))
      }
      List[String]().toIterator
    }.count()
    spark.read.json(outoutFile)
  }

}
