package streaming.core.datasource.impl

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.common.utils.hdfs.HDFSOperator

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLImage(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val baseDir = resourceRealPath(context.execListener, Option(context.owner), config.path)

    if (HDFSOperator.fileExists(baseDir)) {
      if (config.mode == SaveMode.Overwrite) {
        HDFSOperator.deleteDir(baseDir)
      }
      if (config.mode == SaveMode.ErrorIfExists) {
        throw new MLSQLException(s"${baseDir} is exists")
      }
    }

    config.config.get(imageColumn.name).map { m =>
      set(imageColumn, m)
    }.getOrElse {
      throw new MLSQLException(s"${imageColumn.name} is required")
    }

    config.config.get(fileName.name).map { m =>
      set(fileName, m)
    }.getOrElse {
      throw new MLSQLException(s"${fileName.name} is required")
    }

    val _fileName = $(fileName)
    val _imageColumn = $(imageColumn)

    val saveImage = (fileName: String, buffer: Array[Byte]) => {
      HDFSOperator.saveBytesFile(baseDir, fileName, buffer)
      baseDir + "/" + fileName
    }

    config.df.get.rdd.map(r => saveImage(r.getAs[String](_fileName), r.getAs[Array[Byte]](_imageColumn))).count()
  }

  override def fullFormat: String = "streaming.dsl.mmlib.algs.processing.image"

  override def shortFormat: String = "image"

  final val imageColumn: Param[String] = new Param[String](this, "imageColumn", "for save mode")
  final val fileName: Param[String] = new Param[String](this, "fileName", "for save mode")

}

