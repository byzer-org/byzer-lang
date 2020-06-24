package tech.mlsql.ets.ml.cluster

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.WowRowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import streaming.dsl.mmlib.algs.python.LocalPathConfig
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.ets.tensorflow.files.{JsonOutputWriter, ParquetOutputWriter}
import tech.mlsql.log.LogUtils

/**
  * 2019-08-26 WilliamZhu(allwefantasy@gmail.com)
  */
object DataManager extends Logging {
  def setupData(iter: Iterator[Row],
                sourceSchema: StructType,
                sessionLocalTimeZone: String,
                algIndex: Int,
                fileType: String) = {
    val convert = WowRowEncoder.fromRow(sourceSchema)

    val localPathConfig = LocalPathConfig.buildFromParams(null)
    var tempDataLocalPathWithAlgSuffix = localPathConfig.localDataPath
    tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
    val msg = s"dataLocalFormat enabled ,system will generate data in ${tempDataLocalPathWithAlgSuffix} "
    logInfo(LogUtils.format(msg))
    if (!new File(tempDataLocalPathWithAlgSuffix).exists()) {
      FileUtils.forceMkdir(new File(tempDataLocalPathWithAlgSuffix))
    }
    val localFilePrefix = PathFun(tempDataLocalPathWithAlgSuffix).add(UUID.randomUUID().toString).toPath
    val (localFile, localFileWriter) = fileType match {
      case "parquet" => (localFilePrefix + ".snappy.parquet", new ParquetOutputWriter(localFilePrefix + ".snappy.parquet", new Configuration()))
      case "json" => (localFilePrefix + ".json", new JsonOutputWriter(localFilePrefix + ".json", sourceSchema, sessionLocalTimeZone))
    }

    try {
      iter.foreach { row =>
        localFileWriter.write(convert(row))
      }
    } finally {
      localFileWriter.close()
    }
    LocalPathRes(localFile, localPathConfig)
  }


}

case class LocalPathRes(localDataFile: String, localPathConfig: LocalPathConfig)
