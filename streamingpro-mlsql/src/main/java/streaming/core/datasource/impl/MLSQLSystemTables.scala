package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.common.ScalaEnumTool
import streaming.core.StreamingproJobInfo
import streaming.core.datasource._
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{OperateType, TableType}

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemTables extends MLSQLSource with MLSQLSourceInfo with MLSQLRegistry {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    val spark = config.df.get.sparkSession
    import spark.implicits._

    val jobCollect = new MLSQLJobCollect(spark, owner)
    config.path.split("/") match {
      case Array("datasources") => {
        spark.createDataset(DataSourceRegistry.allSourceNames.toSet.toSeq ++ Seq(
          "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
          "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
        )).toDF("name")
      }
      case Array("datasources", "params", item: String) => {
        DataSourceRegistry.fetch(item, Map[String, String]()) match {
          case Some(ds) => ds.asInstanceOf[MLSQLSourceInfo].explainParams(spark)
          case None => spark.createDataset[String](Seq()).toDF("name")
        }

      }
      case Array("jobs") =>
        spark.createDataset[StreamingproJobInfo](jobCollect.jobs).toDF()
      case Array("jobs", jobGroupId) =>
        spark.createDataset(Seq(jobCollect.jobDetail(jobGroupId))).toDF()
      case Array("progress", jobGroupId) =>
        spark.createDataset(jobCollect.jobProgress(jobGroupId)).toDF()
      case Array("resource") =>
        spark.createDataset(Seq(jobCollect.resourceSummary(null))).toDF()
      case Array("resource", jobGroupId) =>
        val detail = jobCollect.jobDetail(jobGroupId)
        detail.activeJobs.map(_.numActiveTasks)
        spark.createDataset(Seq(jobCollect.resourceSummary(jobGroupId))).toDF()

      case Array("tables", "tableTypes") =>
        spark.createDataset(TableType.toList).toDF()

      case Array("tables", "sourceTypes") =>
        spark.createDataset(SourceTypeRegistry.sources).toDF()

      case Array("tables", "operateTypes") =>
        val res = ScalaEnumTool.valueSymbols[OperateType.type].map(f => f.toString.split("\\s+").last.toLowerCase()).toSeq
        spark.createDataset(res).toDF()
    }

  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(fullFormat, fullFormat, "jobs")

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "_mlsql_"

  override def shortFormat: String = "_mlsql_"
}
