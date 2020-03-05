package tech.mlsql.dsl.processor

import org.apache.spark.sql.SparkSession
import streaming.dsl.ScriptSQLExecListener
import streaming.dsl.parser.DSLSQLParser.SqlContext
import tech.mlsql.dsl.adaptor.SelectGrammarAdaptor
import tech.mlsql.dsl.adaptor.LoadGrammarAdaptor

/**
  * 2019-04-12 WilliamZhu(allwefantasy@gmail.com)
  */
class GrammarProcessListener(val sqel: ScriptSQLExecListener, _sparkSession: SparkSession, _defaultPathPrefix: String, _allPathPrefix: Map[String, String]) extends ScriptSQLExecListener(_sparkSession, _defaultPathPrefix, _allPathPrefix) {
  def this(sqel: ScriptSQLExecListener) {
    this(sqel, null, null, null)
  }

  override def exitSql(ctx: SqlContext): Unit = {
    ctx.getChild(0).getText.toLowerCase() match {
      case "select" =>
        new SelectGrammarAdaptor(this).parse(ctx)
      case "load" =>
        new LoadGrammarAdaptor(this).parse(ctx)
      case _ =>
    }
  }
}
