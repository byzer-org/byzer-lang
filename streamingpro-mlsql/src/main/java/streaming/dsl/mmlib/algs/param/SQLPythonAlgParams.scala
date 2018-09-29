package streaming.dsl.mmlib.algs.param

import org.apache.spark.ml.param.{BooleanParam, Param}

/**
  * Created by allwefantasy on 28/9/2018.
  */
trait SQLPythonAlgParams extends BaseParams {

  final val enableDataLocal: BooleanParam = new BooleanParam(this, "enableDataLocal",
    "Save prepared data to HDFS and then copy them to local")
  setDefault(enableDataLocal, false)


  final val dataLocalFormat: Param[String] = new Param[String](this, "dataLocalFormat",
    "dataLocalFormat")
  setDefault(dataLocalFormat, "json")


  final val pythonScriptPath: Param[String] = new Param[String](this, "pythonScriptPath",
    "The location of python script")

  final val kafkaParam_bootstrap_servers: Param[String] = new Param[String](this, "kafkaParam.bootstrap.servers",
    "Set kafka server address")


  final val systemParam_pythonPath: Param[String] = new Param[String](this, "systemParam.pythonPath",
    "Configure python path")

  final val systemParam_pythonParam: Param[String] = new Param[String](this, "systemParam.pythonParam",
    "python params e.g -u")

  final val systemParam_pythonVer: Param[String] = new Param[String](this, "systemParam.pythonVer",
    "Configure python path")

  final val fitParam: Param[String] = new Param[String](this, "fitParam",
    "fitParam is dynamic params. e.g. fitParam.0.moduleName,fitParam.1.moduleName`")

}
