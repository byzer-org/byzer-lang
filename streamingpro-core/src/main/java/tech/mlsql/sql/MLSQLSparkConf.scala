package tech.mlsql.sql

import java.util.HashMap

import streaming.dsl.ScriptSQLExec

/**
  * 2019-05-19 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLSparkConf {

  private[this] val mlsqlConfEntries = new HashMap[String, ConfItem[_]]()

  private def add[T](name: String, doc: String, defaultValue: T) = {
    mlsqlConfEntries.put(name, ConfItem(name, doc, defaultValue))
    name
  }

  def entries = mlsqlConfEntries

  val KEY_RUNTIME_DIRECTQUERY_AUTH = add("spark.mlsql.enable.runtime.directQuery.auth", "", false)
  val KEY_MLSQL_ENABLE_RUNTIME_SELECT_AUTH = add("spark.mlsql.enable.runtime.select.auth", "", false)
  val KEY_MLSQL_ENABLE_DATASOURCE_REWRITE = add("spark.mlsql.enable.datasource.rewrite", "", false)
  val KEY_MLSQL_DATASOURCE_REWRITE_IMPLCLASS = add("spark.mlsql.datasource.rewrite.implClass", "", "")

  private def _conf() = {
    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.sparkSession
      .sparkContext
      .getConf
  }

  def runtimeDirectQueryAuth = {
    _conf().getBoolean(KEY_RUNTIME_DIRECTQUERY_AUTH, false)
  }

  def runtimeSelectAuth = {
    _conf().getBoolean(KEY_MLSQL_ENABLE_RUNTIME_SELECT_AUTH, false)
  }

  def runtimeLoadRewrite = {
    _conf().getBoolean(KEY_MLSQL_ENABLE_DATASOURCE_REWRITE, false)
  }

  def runtimeLoadRewriteImpl = {
    _conf().get(KEY_MLSQL_DATASOURCE_REWRITE_IMPLCLASS, "")
  }

}

case class ConfItem[T](name: String, doc: String, defaultValue: T)
