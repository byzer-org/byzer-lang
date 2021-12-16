/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.core.datasource.impl

import java.util.Properties
import _root_.streaming.core.datasource.{SourceTypeRegistry, _}
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import _root_.streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import _root_.streaming.log.WowLog
import com.alibaba.druid.util.JdbcUtils
import org.apache.spark.{MLSQLSparkConst, SparkCoreVersion}
import org.apache.spark.ml.param.{BooleanParam, LongParam, Param}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import tech.mlsql.common.utils.lang.sc.ScalaReflect
import tech.mlsql.common.utils.hdfs.DistrLocker
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.tool.HDFSOperatorV2

class MLSQLJDBC(override val uid: String) extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry with WowParams with Logging with WowLog {
  def this() = this(BaseParams.randomUID())


  override def fullFormat: String = "jdbc"

  override def shortFormat: String = fullFormat

  override def dbSplitter: String = "."

  def toSplit = "\\."

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    var dbTable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    var driver: Option[String] = config.config.get("driver")
    var url = config.config.get("url")
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbTable) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        dbTable = _dbTable
        reader.options(options)
        url = options.get("url")
        driver = options.get("driver")
      })
    }
    //load configs should overwrite connect configs
    reader.options(config.config)
    assert(url.isDefined, s"url could not be null!")
    assert(driver.isDefined, s"driver could not be null!")
    if (JdbcUtils.isMySqlDriver(driver.get)) {
      /**
       * Fetch Size It's a value for JDBC PreparedStatement.
       * To avoid data overload in the jvm and cause OOM, we set the default value to Integer's MINVALUE
       */
      MLSQLSparkConst.majorVersion(SparkCoreVersion.exactVersion) match {
        case 1 | 2 =>
          reader.options(Map("fetchsize" -> "1000"))
          url = url.map(x => if (x.contains("useCursorFetch")) x else s"$x&useCursorFetch=true")
        case _ =>
          reader.options(Map("fetchsize" -> s"${Integer.MIN_VALUE}"))
      }

      url = url.map(x => if (x.contains("autoReconnect")) x else s"$x&autoReconnect=true")
        .map(x => if (x.contains("failOverReadOnly")) x else s"$x&failOverReadOnly=false")
    }

    val table = if (config.config.contains("prePtnArray")){
      val prePtn = config.config.get("prePtnArray").get
        .split(config.config.getOrElse("prePtnDelimiter" ,","))

      reader.jdbc(url.get, dbTable, prePtn, new Properties())
    }else{
      reader.option("dbtable", dbTable)
      reader.format(format).load()
    }

    val columns = table.columns
    val colNames = new Array[String](columns.length)
    for (i <- 0 to columns.length - 1) {
      val (dbtable, column) = parseTableAndColumnFromStr(columns(i))
      colNames(i) = column
    }
    val newdf = table.toDF(colNames: _*)
    cacheTableInParquet(newdf, config)
  }

  def cacheTableInParquet(table: DataFrame, config: DataSourceConfig): DataFrame = {
    val sourceinfo = sourceInfo(DataAuthConfig(config.path, config.config))
    val sparkSession = table.sparkSession

    val enableCache = table.sparkSession
      .sparkContext
      .getConf
      .getBoolean("spark.mlsql.enable.datasource.mysql.cache", false)

    if (enableCache && sourceinfo.sourceType.toLowerCase() == "mysql") {
      config.config.get(enableCacheToHDFS.name).map { f =>
        set(enableCacheToHDFS, f.toBoolean)
        f.toBoolean
      }.getOrElse {
        set(enableCacheToHDFS, true)
      }

      config.config.get(waitCacheLockTime.name).map { f =>
        set(waitCacheLockTime, f.toLong)
        f.toBoolean
      }.getOrElse {
        set(waitCacheLockTime, 60 * 60 * 3l)
      }

      config.config.get(cacheToHDFSExpireTime.name).map { f =>
        set(cacheToHDFSExpireTime, f.toLong)
        f.toBoolean
      }.getOrElse {
        set(cacheToHDFSExpireTime, 60 * 60 * 6l)
      }

      if ($(enableCacheToHDFS)) {
        val newtableName = sourceinfo.sourceType.toLowerCase() + "_" + sourceinfo.db + "_" + sourceinfo.table

        val context = ScriptSQLExec.context()
        val home = if (context != null) context.home else ""
        val finalPath = s"${home}/tmp/_jdbc_cache_/${newtableName}"

        try {
          HDFSOperatorV2.createDir(finalPath)
        } catch {
          case e: Exception =>
        }


        def isExpire = {
          // we should check the data dir instead of the finalPath since the final path
          // will be written with lock file which may make the modification time changed
          HDFSOperatorV2.fileExists(finalPath + "/data") && System.currentTimeMillis() - HDFSOperatorV2.getFileStatus(finalPath + "/data").getModificationTime > $(cacheToHDFSExpireTime) * 1000
        }

        val hdfsLocker = new DistrLocker(finalPath)
        var newTable: DataFrame = null
        try {
          //create lock file
          hdfsLocker.createLock
          // try to fetch lock
          if (!hdfsLocker.fetchLock()) {
            // fail to fetch lock, then wait until other release the lock
            logInfo(format(s"${finalPath} is locked by other service, wait and then use"))
            hdfsLocker.releaseLock()
            hdfsLocker.waitOtherLockToRelease($(waitCacheLockTime))
            // try to read the file
            newTable = sparkSession.read.parquet(finalPath + "/data")
          } else {
            // succesfully get the lock, then check the file if exists or isExpire
            // if not exits or the table have expire, then remove it and create new one.
            // finally release the lock
            logInfo(format(s"${finalPath} is locked by this service and we will create the data if it not exists"))
            if (!HDFSOperatorV2.fileExists(finalPath + "/data") || isExpire) {
              table.write.mode(SaveMode.Overwrite).save(finalPath + "/data")
            }
            try {
              sparkSession.read.parquet(finalPath + "/data")
            } catch {
              case e: Exception =>
                logInfo(format_exception(e))
                table.write.mode(SaveMode.Overwrite).save(finalPath + "/data")
            }

            try {
              newTable = sparkSession.read.parquet(finalPath + "/data")
            } catch {
              case e: Exception =>
                logWarning(format(s"we try to cache table ${finalPath}, but it fails:"))
                logInfo(format_exception(e))
                newTable = table
            }


          }
        } finally {
          logInfo(format(s"${finalPath} is locked by other service, wait and then use"))
          hdfsLocker.releaseLock()
        }
        return newTable

      }
    }
    return table
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    var dbtable = config.path
    // if contains splitter, then we will try to find dbname in dbMapping.
    // otherwize we will do nothing since elasticsearch use something like index/type
    // it will do no harm.
    val format = config.config.getOrElse("implClass", fullFormat)
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _dbtable) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        dbtable = _dbtable
        writer.options(options)
      })
    }
    writer.mode(config.mode)
    //load configs should overwrite connect configs
    writer.options(config.config)
    config.config.get("partitionByCol").map { item =>
      writer.partitionBy(item.split(","): _*)
    }

    config.config.get("idCol").map { item =>
      import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
      val extraOptions = ScalaReflect.fromInstance[DataFrameWriter[Row]](writer)
        .method("extraOptions").invoke()
        .asInstanceOf[{def toMap[T, U](implicit ev: _ <:< (T, U)): scala.collection.immutable.Map[T, U] }].toMap[String,String]
      val jdbcOptions = new JDBCOptions(extraOptions + ("dbtable" -> dbtable))
      writer.upsert(Option(item), jdbcOptions, config.df.get)
    }.getOrElse {
      writer.option("dbtable", dbtable)
      writer.format(format).save(dbtable)
    }
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  def parseTableAndColumnFromStr(str: String): (String, String) = {
    val cleanedStr = cleanStr(str)
    val dbAndTable = cleanedStr.split("\\.")
    if (dbAndTable.length > 1) {
      val table = dbAndTable(0)
      val column = dbAndTable.splitAt(1)._2.mkString(".")
      (table, column)
    } else {
      (cleanedStr, cleanedStr)
    }
  }

  def cleanStr(str: String): String = {
    if (str.startsWith("`") || str.startsWith("\""))
      str.substring(1, str.length - 1)
    else str
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(toSplit, 2)
    } else {
      Array("", config.path)
    }

    val url = if (config.config.contains("url")) {
      config.config.get("url").get
    } else {
      val format = config.config.getOrElse("implClass", fullFormat)

      ConnectMeta.options(DBMappingKey(format, _dbname)) match {
        case Some(item) => item("url")
        case None => throw new RuntimeException(
          s"""
             |format: ${format}
             |ref:${_dbname}
             |However ref is not found,
             |Have you  set the connect statement properly?
           """.stripMargin)
      }
    }

    val dataSourceType = url.split(":")(1)
    val dbName = url.substring(url.lastIndexOf('/') + 1).takeWhile(_ != '?')
    val si = SourceInfo(dataSourceType, dbName, _dbtable)
    SourceTypeRegistry.register(dataSourceType, si)
    si
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  final val url: Param[String] = new Param[String](this, "url", "The JDBC URL to connect to. The source-specific connection properties may be specified in the URL. e.g., jdbc:postgresql://localhost/test?user=fred&password=secret")
  final val driver: Param[String] = new Param[String](this, "driver", "The class name of the JDBC driver to use to connect to this URL.")
  final val user: Param[String] = new Param[String](this, "user", "")
  final val password: Param[String] = new Param[String](this, "password", "")
  final val partitionColumn: Param[String] = new Param[String](this, "partitionColumn", "These options must all be specified if any of them is specified. In addition, numPartitions must be specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric, date, or timestamp column from the table in question. Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned. This option applies only to reading.")
  final val lowerBound: Param[String] = new Param[String](this, "lowerBound", "See partitionColumn")
  final val upperBound: Param[String] = new Param[String](this, "upperBound", "See partitionColumn")
  final val enableCacheToHDFS: BooleanParam = new BooleanParam(this, "enableCacheToHDFS", "enabled by default in MySQL;The target path is ${HOME}/tmp/_jdbc_cache_")
  final val waitCacheLockTime: LongParam = new LongParam(this, "waitCacheLockTime", "default 30m;unit seconds")
  final val cacheToHDFSExpireTime: LongParam = new LongParam(this, "cacheToHDFSExpireTime", "default 6h; unit seconds")

}

