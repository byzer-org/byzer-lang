package tech.mlsql.dsl.includes

import org.apache.http.client.fluent.Request
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mlsql.session.MLSQLException
import streaming.common.PathFun
import streaming.dsl.IncludeSource
import streaming.log.Logging

/**
  * 2019-05-12 WilliamZhu(allwefantasy@gmail.com)
  */
class StoreIncludeSource extends IncludeSource with Logging {
  override def fetchSource(sparkSession: SparkSession, path: String, options: Map[String, String]): String = {

    val res = Request.Get(PathFun("http://repo.store.mlsql.tech").add(path).toPath)
      .connectTimeout(60 * 1000)
      .socketTimeout(10 * 60 * 1000)
      .execute().returnContent().asString()

    if (res == null) {
      throw new MLSQLException(
        s"""
           |${path} is not found
         """.stripMargin)
    }
    res
  }

  override def skipPathPrefix: Boolean = true
}
