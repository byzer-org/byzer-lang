package tech.mlsql.ets.ml.cluster

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import streaming.dsl.mmlib.algs.python.LocalPathConfig
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.ets.tensorflow.files.ParquetOutputWriter
import tech.mlsql.log.LogUtils

/**
  * 2019-08-26 WilliamZhu(allwefantasy@gmail.com)
  */
object DataManager extends Logging {
  def setupData(iter: Iterator[Row], sourceSchema: StructType, algIndex: Int) = {
    val encoder = RowEncoder.apply(sourceSchema).resolveAndBind()
    val localPathConfig = LocalPathConfig.buildFromParams(null)
    var tempDataLocalPathWithAlgSuffix = localPathConfig.localDataPath
    tempDataLocalPathWithAlgSuffix = tempDataLocalPathWithAlgSuffix + "/" + algIndex
    val msg = s"dataLocalFormat enabled ,system will generate data in ${tempDataLocalPathWithAlgSuffix} "
    logInfo(LogUtils.format(msg))
    if (!new File(tempDataLocalPathWithAlgSuffix).exists()) {
      FileUtils.forceMkdir(new File(tempDataLocalPathWithAlgSuffix))
    }
    val localFile = PathFun(tempDataLocalPathWithAlgSuffix).add(UUID.randomUUID().toString + ".snappy.parquet").toPath
    val localFileWriter = new ParquetOutputWriter(localFile, new Configuration())
    try {
      iter.foreach { row =>
        localFileWriter.write(encoder.toRow(row))
      }
    } finally {
      localFileWriter.close()
    }
    LocalPathRes(localFile, localPathConfig)
  }



}

case class LocalPathRes(localDataFile:String, localPathConfig: LocalPathConfig)
