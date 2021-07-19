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
      val loadPath = (dataLake.isEnable, config.config.get("mode").map(_.toLowerCase())) match {
        case (true, None) =>
          if (config.path.contains("/")) {
            throw new MLSQLException(
              """Delta path mode is not enabled, you can use table mode like "load delta.`public.test` as t1" or add parameter like
                | load delta.`/mlsql/delta/public/test` where mode= "path" """.stripMargin)
          }
          dataLake.identifyToPath(config.path)
        case (true, Some("path")) | (false, _)  =>
          resourceRealPath(context.execListener, Option(owner), config.path)
        case (_, _) =>
          throw new MLSQLException(
            """ Can not resolve mode argument, you can add parameter like
              | load delta.`/mlsql/delta/public/test` where mode= "path"
            """.stripMargin)
      }

      reader.options(rewriteConfig(config.config) ++ newOpt).
        format(format).
        load(loadPath)
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
    val finalPath = (dataLake.isEnable, config.config.get("mode").map(_.toLowerCase())) match {
      case (true, None) =>
        if (config.path.contains("/")) {
          throw new MLSQLException(
            """Delta path mode is not enabled, you can use table mode like "save overwrite test as delta.`public.test`" or add parameter like
              | save overwrite test as delta.`/mlsql/delta/public/test` where mode= "path" """.stripMargin)
        }
        dataLake.identifyToPath(config.path)
      case (true, Some("path")) =>
        if (config.path.startsWith(dataLake.value)) {
          throw new MLSQLException(
            s""" Can not save delta in table mode directory ${dataLake.value}""")
        }
        resourceRealPath(context.execListener, Option(context.owner), config.path)
      case (false, _) =>
        resourceRealPath(context.execListener, Option(context.owner), config.path)
      case (_, _) =>
        throw new MLSQLException(
          """ Can not resolve mode argument, you can add parameter like
            | save test as delta.`/mlsql/delta/public/test` where mode= "path"
          """.stripMargin)
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
