package tech.mlsql.datasource.impl

import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.delta.commands.UpsertTableInDelta
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.streaming.{DataStreamWriter, MLSQLForeachBatchRunner}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SaveMode, functions => F}
import streaming.core.datasource._
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.source.parser.impl.JsonSourceParser
import tech.mlsql.common.utils.Md5
import tech.mlsql.datalake.DataLake
import tech.mlsql.datasource.{MLSQLMultiDeltaOptions, TableMetaInfo}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * 2019-06-17 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLMultiDelta(override val uid: String) extends MLSQLBaseStreamSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    throw new RuntimeException("not support load")
  }

  override def save(batchWriter: DataFrameWriter[Row], config: DataSinkConfig): Any = {
    require(config.config.contains("idCols"), "idCols is required")
    require(config.path.contains("{table}") && config.path.contains("{db}"), "path should like /tmp/db-sync/{db}/{table}")
    val context = ScriptSQLExec.contextGetOrForTest()

    val filePath = resourceRealPath(context.execListener, Option(context.owner), config.path)
    val dataLake = new DataLake(config.df.get.sparkSession)
    val finalPath = if (dataLake.isEnable) {
      dataLake.identifyToPath(filePath)
    } else {
      filePath
    }

    val newConfig = config.copy(
      config = Map("path" -> config.path, MLSQLMultiDeltaOptions.FULL_PATH_KEY -> finalPath) ++ config.config)


    return super.save(batchWriter, newConfig)

  }


  override def foreachBatchCallback(dataStreamWriter: DataStreamWriter[Row], options: Map[String, String]): Unit = {

    MLSQLForeachBatchRunner.run(dataStreamWriter, (ds, batchId) => {

      // Notice that for now, ds is not really can be re-consumed. This means we should cache it
      ds.cache()
      try {
        if (options.getOrElse(MLSQLMultiDeltaOptions.KEEP_BINLOG, "false").toBoolean) {
          val originalLogPath = options(MLSQLMultiDeltaOptions.BINLOG_PATH)
          ds.write.format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").mode(SaveMode.Append).save(originalLogPath)
        } else {
          // do cache
          ds.count()
        }

        val idCols = options(UpsertTableInDelta.ID_COLS).split(",").toSeq

        def _getInfoFromMeta(record: JSONObject, key: String) = {
          record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY).getString(key)
        }

        def getDatabaseNameFromMeta(record: JSONObject) = {
          _getInfoFromMeta(record, "databaseName")
        }

        def getTableNameNameFromMeta(record: JSONObject) = {
          _getInfoFromMeta(record, "tableName")
        }

        def getschemaNameFromMeta(record: JSONObject) = {
          _getInfoFromMeta(record, "schema")
        }

        val spark = ds.sparkSession
        import spark.implicits._
        val dataSet = ds.rdd.flatMap { row =>
          val value = row.getString(0)
          val wow = JSONObject.fromObject(value)
          val rows = wow.remove("rows")
          rows.asInstanceOf[JSONArray].asScala.map { record =>
            record.asInstanceOf[JSONObject].put(MLSQLMultiDeltaOptions.META_KEY, wow)
            record.asInstanceOf[JSONObject]
          }
        }

        val finalDataSet = dataSet.map { record =>
          val idColKey = idCols.map { idCol =>
            record.get(idCol).toString
          }.mkString("")
          val key = Md5.md5Hash(getDatabaseNameFromMeta(record) + "_" + getTableNameNameFromMeta(record) + "_" + idColKey)
          (key, record.toString)
        }.groupBy(_._1).map { f => f._2.map(m => JSONObject.fromObject(m._2)) }.map { records =>
          // we get the same record operations, and sort by timestamp, get the last operation
          val items = records.toSeq.sortBy(record => record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY).getLong("timestamp"))
          items.last
        }

        val tableToId = finalDataSet.map { record =>
          TableMetaInfo(getDatabaseNameFromMeta(record), getTableNameNameFromMeta(record), getschemaNameFromMeta(record))
        }.distinct().collect().zipWithIndex.toMap

        def saveToSink(targetRDD: RDD[JSONObject], operate: String) = {
          tableToId.map { case (table, index) =>
            val tempRDD = targetRDD.filter(record => getDatabaseNameFromMeta(record) == table.db && getTableNameNameFromMeta(record) == table.table).map { record =>
              record.remove(MLSQLMultiDeltaOptions.META_KEY)
              record.toString
            }

            def deserializeSchema(json: String): StructType = {
              DataType.fromJson(json) match {
                case t: StructType => t
                case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
              }
            }

            val sourceSchema = deserializeSchema(table.schema)
            val sourceParserInstance = new JsonSourceParser()

            val deleteDF = spark.createDataset[String](tempRDD).toDF("value").select(sourceParserInstance.parseRaw(F.col("value"), sourceSchema, Map()).as("data"))
              .select("data.*")
            val path = options(MLSQLMultiDeltaOptions.FULL_PATH_KEY)
            val tablePath = path.replace("{db}", table.db).replace("{table}", table.table)
            val deltaLog = DeltaLog.forTable(spark, tablePath)

            val readVersion = deltaLog.snapshot.version
            val isInitial = readVersion < 0
            if (isInitial) {
              throw new RuntimeException(s"${tablePath} is not initialed")
            }

            val upsertTableInDelta = new UpsertTableInDelta(deleteDF,
              Some(SaveMode.Append),
              None,
              deltaLog,
              options = new DeltaOptions(Map[String, String](), ds.sparkSession.sessionState.conf),
              partitionColumns = Seq(),
              configuration = options ++ Map(
                UpsertTableInDelta.OPERATION_TYPE -> operate,
                UpsertTableInDelta.ID_COLS -> idCols.mkString(",")
              )
            )
            upsertTableInDelta.run(spark)

          }
        }

        // filter upsert
        val upsertRDD = finalDataSet.filter { record =>
          val meta = record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY)
          meta.getString("type") != "delete"
        }
        if (upsertRDD.count() > 0) {
          saveToSink(upsertRDD, UpsertTableInDelta.OPERATION_TYPE_UPSERT)
        }

        // filter delete

        val deleteRDD = finalDataSet.filter { record =>
          val meta = record.getJSONObject(MLSQLMultiDeltaOptions.META_KEY)
          meta.getString("type") == "delete"
        }
        if (deleteRDD.count() > 0) {
          saveToSink(deleteRDD, UpsertTableInDelta.OPERATION_TYPE_DELETE)
        }
      } finally {
        ds.unpersist()
      }
    })
  }

  override def resolvePath(path: String, owner: String): String = {
    val context = ScriptSQLExec.contextGetOrForTest()
    resourceRealPath(context.execListener, Option(owner), path)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = config.config.get("owner").getOrElse(context.owner)
    SourceInfo(shortFormat, "", resourceRealPath(context.execListener, Option(owner), config.path))
  }

  override def fullFormat: String = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  override def shortFormat: String = "binlogRate"

}


