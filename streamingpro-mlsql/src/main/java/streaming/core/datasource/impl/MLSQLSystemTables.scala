package streaming.core.datasource.impl

import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import streaming.core.datasource._
import streaming.core.datasource.util.MLSQLJobCollect
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{OperateType, TableType}
import streaming.dsl.load.batch.{LogTail, MLSQLAPIExplain, MLSQLConfExplain}
import tech.mlsql.MLSQLEnvKey
import tech.mlsql.core.version.MLSQLVersion
import tech.mlsql.job.MLSQLJobInfo

/**
  * 2019-01-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLSystemTables extends MLSQLSource with MLSQLSourceInfo with MLSQLRegistry {

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest();
    val owner = context.owner
    context.execListener.addEnv(MLSQLEnvKey.CONTEXT_SYSTEM_TABLE, "true")
    val spark = config.df.get.sparkSession
    import spark.implicits._

    val jobCollect = new MLSQLJobCollect(spark, owner)

    val pathSplitter = "/"

    config.path.stripPrefix(pathSplitter).stripSuffix(pathSplitter).split(pathSplitter) match {
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
        spark.createDataset[MLSQLJobInfo](jobCollect.jobs).toDF()
      case Array("jobs", jobGroupId) =>
        spark.createDataset(Seq(jobCollect.jobDetail(jobGroupId))).toDF()
      case Array("jobs","v2" ,jobGroupId) =>
        spark.createDataset(Seq(jobCollect.jobDetail(jobGroupId,2))).toDF()
      case Array("jobs", "get", jobGroupId) =>
        spark.createDataset[MLSQLJobInfo](jobCollect.getJob(jobGroupId)).toDF()
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
        spark.createDataset(SourceTypeRegistry.sources ++ TableType.toIncludesList).toDF()

      case Array("tables", "operateTypes") =>
        val res = OperateType.toList
        spark.createDataset(res).toDF()
      case Array("api", "list") =>
        new MLSQLAPIExplain(spark).explain
      case Array("conf", "list") =>
        new MLSQLConfExplain(spark).explain
      case Array("log", offset) =>
        val filePath = config.config.getOrElse("filePath", "")
        val msgs = LogTail.log(owner, filePath, offset.toLong)
        spark.createDataset(Seq(msgs)).toDF("offset", "value")
      case Array("version") =>
        spark.createDataset(Seq(MLSQLVersion.version())).toDF()

      case _ => throw new MLSQLException(
        s"""
           |path [${config.path}] is not found. please check the doc website for more details:
           |http://docs.mlsql.tech/zh
           |or
           |http://docs.mlsql.tech/en
         """.stripMargin)
    }

  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = SourceInfo(fullFormat, "mlsql_system_db", "system_info")

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "_mlsql_"

  override def shortFormat: String = "_mlsql_"
}
