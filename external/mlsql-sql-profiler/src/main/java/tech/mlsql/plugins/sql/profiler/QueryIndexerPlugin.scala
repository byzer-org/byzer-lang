package tech.mlsql.plugins.sql.profiler

import org.apache.spark.sql.DataSetHelper
import streaming.dsl.ScriptSQLExec
import tech.mlsql.app.{ResultRender, ResultResp}
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.indexer.impl.{LinearTryIndexerSelector, RestIndexerMeta, ZOrderingIndexer}


class IndexerPlugin extends ResultRender {
  override def call(d: ResultResp): ResultResp = {
    val params = JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam.getOrElse("__PARAMS__", "{}"))
    if (!params.getOrElse("enableQueryWithIndexer", "false").toBoolean) {
      return d
    }
    val consoleUrl = ScriptSQLExec.context().userDefinedParam.getOrElse("__default__console_url__", "")
    val auth_secret = ScriptSQLExec.context().userDefinedParam.getOrElse("__auth_secret__", "")
    val metaClient = new RestIndexerMeta(consoleUrl, auth_secret)
    val indexer = new LinearTryIndexerSelector(Seq(new ZOrderingIndexer), metaClient)
    val finalLP = indexer.rewrite(d.df.queryExecution.analyzed, Map())
    val sparkSession = ScriptSQLExec.context().execListener.sparkSession
    val ds = DataSetHelper.create(sparkSession, finalLP)
    ResultResp(ds, d.name)
  }
}