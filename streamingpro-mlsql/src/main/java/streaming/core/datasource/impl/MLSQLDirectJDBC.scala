package streaming.core.datasource.impl

import java.util.Properties

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.repository.SchemaRepository
import com.alibaba.druid.sql.visitor.SchemaStatVisitor
import com.alibaba.druid.util.{JdbcConstants, JdbcUtils}
import org.apache.spark.sql.catalyst.plans.logical.MLSQLDFParser
import org.apache.spark.sql.execution.WowTableIdentifier
import org.apache.spark.sql.mlsql.session.{MLSQLException, MLSQLSparkSession}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import org.apache.spark.{MLSQLSparkConst, SparkCoreVersion}
import streaming.core.datasource._
import streaming.dsl.auth.{MLSQLTable, OperateType, TableType}
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import streaming.log.WowLog
import tech.mlsql.Stage
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.dsl.auth.DatasourceAuth
import tech.mlsql.sql.{MLSQLSQLParser, MLSQLSparkConf}

import scala.collection.JavaConverters._

/**
 * 2018-12-21 WilliamZhu(allwefantasy@gmail.com)
 */
class MLSQLDirectJDBC extends MLSQLDirectSource with MLSQLDirectSink with MLSQLSourceInfo with MLSQLRegistry
  with DatasourceAuth with Logging with WowLog {

  override def fullFormat: String = "jdbc"

  override def shortFormat: String = fullFormat

  override def dbSplitter: String = "."

  def toSplit = "\\."

  private def loadConfigFromExternal(params: Map[String, String], path: String) = {
    var _params = params
    var isRealDB = true
    if (path.contains(".")) {
      val Array(db, table) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
        isRealDB = false
      })
    }
    _params
  }


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val format = config.config.getOrElse("implClass", fullFormat)
    var driver: Option[String] = config.config.get("driver")
    var url = config.config.get("url")
    if (config.path.contains(dbSplitter)) {
      val Array(_dbname, _) = config.path.split(toSplit, 2)
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
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
        case _ =>
          reader.options(Map("fetchsize" -> s"${Integer.MIN_VALUE}"))
      }

      url = url.map(x => if (x.contains("useCursorFetch")) x else s"$x&useCursorFetch=true")
        .map(x => if (x.contains("autoReconnect")) x else s"$x&autoReconnect=true")
        .map(x => if (x.contains("failOverReadOnly")) x else s"$x&failOverReadOnly=false")
    }

    val dbtable = "(" + config.config("directQuery") + ") temp"

    if (config.config.contains("prePtnArray")) {
      val prePtn = config.config.get("prePtnArray").get
        .split(config.config.getOrElse("prePtnDelimiter", ","))

      reader.jdbc(url.get, dbtable, prePtn, new Properties())
    } else {
      reader.option("dbtable", dbtable)

      reader.format(format).load()
    }
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new MLSQLException("not support yet....")
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLDirectDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLDirectDataSourceType), this)
  }

  override def unRegister(): Unit = {
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(fullFormat, MLSQLDirectDataSourceType))
    DataSourceRegistry.unRegister(MLSQLDataSourceKey(shortFormat, MLSQLDirectDataSourceType))
  }

  def sourceInfo(config: DataAuthConfig): SourceInfo = {
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
             |Have you set the connect statement properly?
           """.stripMargin)
      }
    }

    val dataSourceType = url.split(":")(1)
    val dbName = url.substring(url.lastIndexOf('/') + 1).takeWhile(_ != '?')
    val si = SourceInfo(dataSourceType, dbName, _dbtable)
    SourceTypeRegistry.register(dataSourceType, si)

    si
  }

  // this function depends on druid, so we can
  // fix chinese tablename since in spark parser it's not supported
  //MLSQLAuthParser.filterTables(sql, context.execListener.sparkSession)
  def extractTablesFromSQL(sql: String, dbType: String = JdbcConstants.MYSQL) = {
    val repository = new SchemaRepository(dbType)
    repository.console(sql)
    val stmt = SQLUtils.parseSingleStatement(sql, dbType)
    val visitor = new SchemaStatVisitor()
    stmt.accept(visitor)
    visitor.getTables().asScala.map { f =>
      val dbAndTable = f._1.getName
      if (dbAndTable.contains(".")) {
        val Array(db, table) = dbAndTable.split("\\.", 2)
        WowTableIdentifier(table, Option(db), None)
      } else WowTableIdentifier(dbAndTable, None, None)
    }.toList
  }

  override def auth(path: String, params: Map[String, String]): List[MLSQLTable] = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val enableDirectQueryAuth = context.execListener.sparkSession
      .conf.get("spark.mlsql.directquery.auth.enable", "true").toBoolean

    if (!enableDirectQueryAuth) {
      return List()
    }


    val si = this.sourceInfo(DataAuthConfig(path, params))


    // we should auth all tables in direct query
    val sql = params("directQuery")
    val dbType = params.getOrElse("dbType", JdbcConstants.MYSQL)
    // first, only select supports
    if (!sql.trim.toLowerCase.startsWith("select")) {
      throw new MLSQLException("JDBC direct query only support select statement")
    }
    logInfo("Auth direct query tables.... ")

    def isSkipAuth = {
      context.execListener.env()
        .getOrElse("SKIP_AUTH", "true")
        .toBoolean
    }


    if (!isSkipAuth && context.execListener.getStage.get == Stage.auth && !MLSQLSparkConf.runtimeDirectQueryAuth) {

      //second, we should extract all tables from the sql
      val tableRefs = extractTablesFromSQL(sql, dbType)
      tableRefs.foreach { tableIdentify =>
        if (tableIdentify.database.isDefined) {
          throw new MLSQLException("JDBC direct query should not allow using db prefix. Please just use table")
        }
      }

      val _params = loadConfigFromExternal(params, path)
      val tableList = tableRefs.map(_.identifier)
      val tableColsMap = JDBCUtils.queryTableWithColumnsInDriver(_params, tableList)
      val createSqlList = JDBCUtils.tableColumnsToCreateSql(tableColsMap)
      val tableAndCols = MLSQLSQLParser.extractTableWithColumns(si.sourceType, sql, createSqlList)

      tableAndCols.map { tc =>
        MLSQLTable(Option(si.db), Option(tc._1), Option(tc._2.toSet), OperateType.DIRECT_QUERY, Option(si.sourceType), TableType.JDBC)
      }.foreach { mlsqlTable =>
        context.execListener.authProcessListner match {
          case Some(authProcessListener) =>
            authProcessListener.addTable(mlsqlTable)
          case None =>
        }
      }
    }

    if (!isSkipAuth && context.execListener.getStage.get == Stage.physical && MLSQLSparkConf.runtimeDirectQueryAuth) {

      //second, we should extract all tables from the sql
      val tableRefs = extractTablesFromSQL(sql, dbType)

      tableRefs.foreach { tableIdentify =>
        if (tableIdentify.database.isDefined) {
          throw new MLSQLException("JDBC direct query should not allow using db prefix. Please just use table")
        }
      }

      val _params = loadConfigFromExternal(params, path)
      //clone new sparksession so we will not affect current spark context catalog
      val spark = MLSQLSparkSession.cloneSession(context.execListener.sparkSession)
      tableRefs.map { tableIdentify =>
        spark.read.options(_params + ("dbtable" -> tableIdentify.table)).format("jdbc").load().createOrReplaceTempView(tableIdentify.table)
      }

      val df = spark.sql(sql)
      val tableAndCols = MLSQLDFParser.extractTableWithColumns(df)
      var mlsqlTables = List.empty[MLSQLTable]

      tableAndCols.foreach {
        case (table, cols) =>
          mlsqlTables ::= MLSQLTable(Option(si.db), Option(table), Option(cols.toSet), OperateType.DIRECT_QUERY, Option(si.sourceType), TableType.JDBC)
      }
      context.execListener.getTableAuth.foreach { tableAuth =>
        tableAuth.auth(mlsqlTables)
      }

    }
    List()
  }
}
