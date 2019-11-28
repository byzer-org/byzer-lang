package streaming.core.datasource.impl

import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.datalake.DataLake
import tech.mlsql.datasource.MLSQLMultiDeltaOptions

/**
  * 2019-04-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDelta(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  override def shortFormat: String = "delta"

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val owner = config.config.get("owner").getOrElse(context.owner)

    //timestampAsOf
    val parameters = config.config

    def paramValidate = {
      new MLSQLException("  Both startingVersion,endingVersion are  required")
    }

    def buildDF(version: Long) = {
      val newOpt = if (version > 0) Map("versionAsOf" -> version.toString) else Map()
      val reader = config.df.get.sparkSession.read

      val dataLake = new DataLake(config.df.get.sparkSession)
      val finalPath = if (dataLake.isEnable) {
        dataLake.identifyToPath(config.path)
      } else {
        resourceRealPath(context.execListener, Option(owner), config.path)
      }

      reader.options(rewriteConfig(config.config) ++ newOpt).
        format(format).
        load(finalPath)
    }

    (parameters.get("startingVersion").map(_.toLong), parameters.get("endingVersion").map(_.toLong)) match {
      case (None, None) => buildDF(-1)
      case (None, Some(_)) => throw paramValidate
      case (Some(_), None) => throw paramValidate
      case (Some(start), Some(end)) =>
        (start until end).map { version =>
          buildDF(version).withColumn("__delta_version__", F.lit(version))
        }.reduce((total, cur) => total.union(cur))
    }
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    val context = ScriptSQLExec.contextGetOrForTest()
    val format = config.config.getOrElse("implClass", fullFormat)
    val partitionByCol = config.config.getOrElse("partitionByCol", "").split(",").filterNot(_.isEmpty)
    if (partitionByCol.length > 0) {
      writer.partitionBy(partitionByCol: _*)
    }

    val dataLake = new DataLake(config.df.get.sparkSession)
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(config.path)
    } else {
      resourceRealPath(context.execListener, Option(context.owner), config.path)
    }
    writer.options(rewriteConfig(config.config)).mode(config.mode).format(format).save(finalPath)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val spark = context.execListener.sparkSession
    val dataLake = new DataLake(spark)
    val owner = config.config.get("owner").getOrElse(context.owner)
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(config.path)
    } else {
      resourceRealPath(context.execListener, Option(owner), config.path)
    }
    SourceInfo(shortFormat, "", finalPath)
  }
}


class MLSQLRate(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {

    val streamReader = config.df.get.sparkSession.readStream
    val format = config.config.getOrElse("implClass", fullFormat)
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)

    val dataLake = new DataLake(config.df.get.sparkSession)
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(config.path)
    } else {
      resolvePath(config.path, owner)
    }

    streamReader.options(config.config).format(format).load(finalPath)
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    val dataLake = new DataLake(config.df.get.sparkSession)
    val context = ScriptSQLExec.contextGetOrForTest()
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(config.path)
    } else {
      resolvePath(config.path, context.owner)
    }
    val newConfig = config.copy(
      config = Map("path" -> config.path, MLSQLMultiDeltaOptions.FULL_PATH_KEY -> finalPath) ++ config.config ++ Map("dbtable" -> finalPath))
    return super.save(batchWriter, newConfig)

  }


  override def resolvePath(path: String, owner: String): String = {
    val context = ScriptSQLExec.contextGetOrForTest()
    resourceRealPath(context.execListener, Option(owner), path)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val spark = context.execListener.sparkSession
    val dataLake = new DataLake(spark)
    val owner = config.config.get("owner").getOrElse(context.owner)
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(config.path)
    } else {
      resourceRealPath(context.execListener, Option(owner), config.path)
    }
    SourceInfo(shortFormat, "", finalPath)
  }

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  override def shortFormat: String = "rate"

}
