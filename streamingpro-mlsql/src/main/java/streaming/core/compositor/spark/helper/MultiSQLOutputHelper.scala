package streaming.core.compositor.spark.helper

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by allwefantasy on 16/7/2017.
  */
object MultiSQLOutputHelper {
  def output(_cfg: Map[String, String], spark: SparkSession) = {
    val tableName = _cfg("inputTableName")
    val outputFileNum = _cfg.getOrElse("outputFileNum", "-1").toInt
    val partitionBy = _cfg.getOrElse("partitionBy", "")
    var newTableDF = spark.table(tableName)

    if (outputFileNum != -1) {
      newTableDF = newTableDF.repartition(outputFileNum)
    }

    val _resource = _cfg("path")
    val options = _cfg - "path" - "mode" - "format"
    val dbtable = if (options.contains("dbtable")) options("dbtable") else _resource
    val mode = _cfg.getOrElse("mode", "ErrorIfExists")
    val format = _cfg("format")

    if (format == "console") {
      newTableDF.show(_cfg.getOrElse("showNum", "100").toInt)
    } else {

      var tempDf = if (!partitionBy.isEmpty) {
        newTableDF.write.partitionBy(partitionBy.split(","): _*)
      } else {
        newTableDF.write
      }

      tempDf = tempDf.options(options).mode(SaveMode.valueOf(mode)).format(format)

      if (_resource == "-" || _resource.isEmpty) {
        tempDf.save()
      } else tempDf.save(_resource)
    }
  }
}
