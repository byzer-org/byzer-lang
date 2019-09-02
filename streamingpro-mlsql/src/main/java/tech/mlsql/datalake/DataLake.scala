package tech.mlsql.datalake


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.WowTableIdentifier
import org.apache.spark.sql.mlsql.session.MLSQLException
import tech.mlsql.common.utils.path.PathFun

/**
  * 2019-08-31 WilliamZhu(allwefantasy@gmail.com)
  */
class DataLake(sparkSession: SparkSession) {
  def isEnable = sparkSession.sessionState.conf.contains(DataLake.RUNTIME_KEY)

  def value = {
    sparkSession.sessionState.conf.getConfString(DataLake.RUNTIME_KEY)
  }

  def dbAndtableToPath(db: String, table: String) = {
    PathFun(value).add(db).add(table).toPath
  }

  def identifyToPath(dbAndTable: String) = {
    dbAndTable.split("\\.") match {
      case Array(db, table) => dbAndtableToPath(db, table)
      case Array(table) => dbAndtableToPath("default", table)
      case _ => throw new MLSQLException(s"datalake table format error:${dbAndTable}")
    }
  }

  def listTables = {
    listPath(new Path(value)).flatMap { db =>
      val dbName = db.getPath.getName
      listPath(db.getPath).map { tablePath =>
        val tableName = tablePath.getPath.getName
        WowTableIdentifier(tableName, Option(dbName), None)
      }
    }
  }

  private def listPath(path: Path) = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(path)
  }
}

object DataLake {
  val RUNTIME_KEY = "spark.mlsql.datalake.path"
  val USER_KEY = "streaming.datalake.path"
}
