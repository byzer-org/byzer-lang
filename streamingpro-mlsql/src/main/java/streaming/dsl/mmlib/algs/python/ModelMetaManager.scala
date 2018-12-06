package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.algs.MetaConst
import streaming.dsl.mmlib.algs.SQLPythonFunc._
import streaming.log.{Logging, WowLog}

class ModelMetaManager(sparkSession: SparkSession, _path: String, params: Map[String, String]) extends Logging with WowLog {

  val wowMetas = sparkSession.read.parquet(metaPath + "/1").collect()

  def loadMetaAndModel = {
    val _trainParams = trainParams
    val pythonTrainScript = PythonAlgProject.loadProject(_trainParams, sparkSession)
    ModelMeta(pythonTrainScript.get, _trainParams, modelEntityPaths, Map())
  }

  def maxVersion = getModelVersion(_path)

  def versionEnabled = maxVersion match {
    case Some(v) => true
    case None => false
  }

  def modelVersion = params.getOrElse("modelVersion", maxVersion.getOrElse(-1).toString).toInt

  def metaPath = {
    if (modelVersion == -1) getAlgMetalPath(_path, versionEnabled)
    else getAlgMetalPathWithVersion(_path, modelVersion)
  }

  def modelPath = if (modelVersion == -1) getAlgModelPath(_path, versionEnabled)
  else getAlgModelPathWithVersion(_path, modelVersion)

  def modelEntityPaths = {
    var algIndex = params.getOrElse("algIndex", "-1").toInt
    val modelList = sparkSession.read.parquet(metaPath + "/0").collect()
    val models = if (algIndex != -1) {
      Seq(modelPath + "/" + algIndex)
    } else {
      modelList.map(f => (f(3).asInstanceOf[Double], f(0).asInstanceOf[String], f(1).asInstanceOf[Int]))
        .toSeq
        .sortBy(f => f._1)(Ordering[Double].reverse)
        .take(1)
        .map(f => {
          algIndex = f._3
          modelPath + "/" + f._2.split("/").last
        })
    }
    models
  }

  def trainParams = {

    import sparkSession.implicits._

    var trainParams = Map[String, String]()

    def getTrainParams(isNew: Boolean) = {
      if (isNew)
        wowMetas.map(f => f.getMap[String, String](1)).head.toMap
      else {
        val df = sparkSession.read.parquet(MetaConst.PARAMS_PATH(metaPath, "params")).map(f => (f.getString(0), f.getString(1)))
        df.collect().toMap
      }
    }

    if (versionEnabled) {
      trainParams = getTrainParams(true)
    }

    try {
      trainParams = getTrainParams(false)
    } catch {
      case e: Exception =>
        logInfo(format(s"no directory: ${MetaConst.PARAMS_PATH(metaPath, "params")} ; using ${metaPath + "/1"}"))
        trainParams = getTrainParams(true)
    }
    trainParams
  }


}
