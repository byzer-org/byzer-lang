package streaming.dsl.mmlib.algs.python

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.util.ObjPickle
import streaming.dsl.mmlib.algs.SQLPythonFunc
import streaming.log.{Logging, WowLog}

class DataManager(df: DataFrame, path: String, params: Map[String, String]) extends Logging with WowLog {

  def enableDataLocal = {
    params.getOrElse("enableDataLocal", "false").toBoolean
  }

  def saveDataToHDFS = {
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
    dataHDFSPath
  }

  def broadCastValidateTable = {
    val schema = df.schema
    var rows = Array[Array[Byte]]()
    //目前我们只支持同一个测试集
    if (params.contains("validateTable") || params.contains("evaluateTable")) {
      val validateTable = params.getOrElse("validateTable", params.getOrElse("evaluateTable", ""))
      rows = df.sparkSession.table(validateTable).rdd.mapPartitions { iter =>
        ObjPickle.pickle(iter, schema)
      }.collect()
    }
    df.sparkSession.sparkContext.broadcast(rows)
  }

}
