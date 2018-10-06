package streaming.dsl.mmlib.algs.python

import net.sf.json.JSONObject
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.algs.SQLPythonFunc

import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 30/9/2018.
  */

object RichCaseClass {
  implicit def toMap(cc: Product) = {
    val values = cc.productIterator
    cc.getClass.getDeclaredFields.map(_.getName -> values.next).toMap
  }
}

case class PythonScript(fileName: String,
                        fileContent: String,
                        filePath: String,
                        projectName: String,
                        scriptType: PythonScriptType = Script)

sealed class PythonScriptType

case object Script extends PythonScriptType

case object MLFlow extends PythonScriptType

case object NormalProject extends PythonScriptType

case class MLFlowConfig(mlflowPath: String, mlflowCommand: String, mlflowParam: Seq[String])

object MLFlowConfig {
  def buildFromSystemParam(systemParam: Map[String, String]) = {
    val mlflowPath = systemParam.getOrElse("mlflowPath", "mlflow")
    val mlflowCommand = systemParam.getOrElse("mlflowCommand", "run")
    val mlflowParam = systemParam.getOrElse("mlflowParam", "").split(",").filterNot(f => f.isEmpty)
    MLFlowConfig(mlflowPath, mlflowCommand, mlflowParam)
  }
}

case class PythonConfig(pythonPath: String, pythonParam: Seq[String], pythonVer: String)

object PythonConfig {
  def buildFromSystemParam(systemParam: Map[String, String]) = {
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val pythonVer = systemParam.getOrElse("pythonVer", "3.6")
    val pythonParam = systemParam.getOrElse("pythonParam", "").split(",").filterNot(f => f.isEmpty)
    PythonConfig(pythonPath, pythonParam, pythonVer)
  }
}

case class EnvConfig(envs: Map[String, String])

object EnvConfig {
  def buildFromSystemParam(systemParam: Map[String, String]) = {
    JSONObject.fromObject(systemParam.getOrElse("envs", "{}")).asScala.map(f => (f._1.toString, f._2.toString)).toMap
  }
}

case class LocalPathConfig(localModelPath: String,
                           localTmpPath: String,
                           localDataPath: String,
                           localRunPath: String,
                           localOutputPath: String
                          )

object RunPythonConfig {

  val internalSystemParam = "internalSystemParam"
  val systemParam = "systemParam"

  case class InternalSystemParam(stopFlagNum: Int,
                                 tempModelLocalPath: String,
                                 tempDataLocalPath: String,
                                 tempOutputLocalPath: String,
                                 resource: Map[String, String])

  case class SystemParam(systemParam: Map[String, String])

}

object LocalPathConfig {
  def buildFromParams(path: String) = {
    LocalPathConfig(
      localDataPath = SQLPythonFunc.getLocalTempDataPath(path),
      localTmpPath = SQLPythonFunc.getAlgTmpPath(path),
      localModelPath = SQLPythonFunc.getLocalTempModelPath(path),
      localRunPath = SQLPythonFunc.getLocalRunPath(path),
      localOutputPath = SQLPythonFunc.getLocalTempModelPath(path)
    )
  }
}


case class DataLocalizeConfig(dataLocalFormat: String, dataLocalFileNum: Int = -1, option: Map[String, String] = Map())

object DataLocalizeConfig {
  def buildFromParams(params: Map[String, String]) = {
    val dataLocalFormat = params.getOrElse("dataLocalFormat", "json")
    val dataLocalFileNum = params.getOrElse("dataLocalFileNum", "-1").toInt
    val option = params.filter(f => f._1.startsWith("dataLocal.option.")).map { f =>
      val optionKey = f._1.split("\\.").last
      (optionKey, f._2)
    }
    DataLocalizeConfig(dataLocalFormat, dataLocalFileNum, option)
  }
}

object PythonTrainingResultSchema {
  val algSchema = StructType(Seq(
    StructField("modelPath", StringType),
    StructField("algIndex", IntegerType),
    StructField("alg", StringType),
    StructField("score", DoubleType),

    StructField("status", StringType),
    StructField("startTime", LongType),
    StructField("endTime", LongType),
    StructField("trainParams", MapType(StringType, StringType))
  ))

  val trainParamsSchema = StructType(Seq(
    StructField("systemParam", MapType(StringType, StringType)),
    StructField("trainParams", MapType(StringType, StringType))))
}

case class ModelMeta(pythonScript: PythonScript,
                     trainParams: Map[String, String],
                     modelEntityPaths: Seq[String],
                     resources: Map[String, Any],
                     taskDirectory: Option[String] = None
                    )
