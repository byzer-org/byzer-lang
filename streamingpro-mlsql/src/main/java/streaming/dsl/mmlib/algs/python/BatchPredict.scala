package streaming.dsl.mmlib.algs.python

import java.io.{File, FileWriter}
import java.util

import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.util.PythonProjectExecuteRunner
import streaming.common.ScalaMethodMacros.str
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.{Functions, SQLPythonAlg}
import streaming.log.{Logging, WowLog}

import scala.collection.JavaConverters._

class BatchPredict extends Logging with WowLog with Serializable {
  def predict(df: DataFrame, _path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val modelMetaManager = new ModelMetaManager(spark, _path, params)
    val modelMeta = modelMetaManager.loadMetaAndModel
    val localPathConfig = LocalPathConfig.buildFromParams(_path)
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

    val jsonRDD = df.rdd.mapPartitionsWithIndex { (index, iter) =>
      ScriptSQLExec.setContext(mlsqlContext)
      val mlflowConfig = MLFlowConfig.buildFromSystemParam(systemParam)
      val pythonConfig = PythonConfig.buildFromSystemParam(systemParam)
      val envs = EnvConfig.buildFromSystemParam(systemParam)

      val command = new PythonAlgExecCommand(pythonProject.get, Option(mlflowConfig), Option(pythonConfig)).
        generateCommand(MLProject.batch_predict_command)

      val localPathConfig = LocalPathConfig.buildFromParams(_path)
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

      val internalSystemParam = Map(
        str[RunPythonConfig.InternalSystemParam](_.tempDataLocalPath) -> (localPathConfig.localDataPath + s"/${index}.json"),
        str[RunPythonConfig.InternalSystemParam](_.tempOutputLocalPath) -> (localPathConfig.localOutputPath + "/" + index + "/output.json"),
        str[RunPythonConfig.InternalSystemParam](_.tempModelLocalPath) -> modelPath
      )


      val localOutputPathFile = new File(localPathConfig.localOutputPath + "/" + index)
      if (!localOutputPathFile.exists()) {
        localOutputPathFile.mkdirs()
      }

      paramMap.put(RunPythonConfig.internalSystemParam, (resources ++ internalSystemParam).asJava)
      paramMap.put(RunPythonConfig.systemParam, systemParam.asJava)

      val taskDirectory = localPathConfig.localRunPath + "/" + projectName

      new SQLPythonAlg().downloadPythonProject(taskDirectory, Option(pythonProject.get.filePath))

      val runner = new PythonProjectExecuteRunner(
        taskDirectory = taskDirectory,
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
      } catch {
        case e: Exception =>
          logError(format_cause(e))
          e.printStackTrace()
          trainFailFlag = true
      }
      scala.io.Source.fromFile(localPathConfig.localOutputPath + "/" + index + "/output.json").getLines.toIterator
    }
    spark.read.json(spark.createDataset(jsonRDD)(Encoders.STRING))
  }

}
