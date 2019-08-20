package tech.mlsql.dsl.adaptor

import org.antlr.v4.runtime.misc.Interval
import streaming.dsl.parser.{DSLSQLLexer, DSLSQLParser}
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.processor.GrammarProcessListener

/**
  * 2019-04-12 WilliamZhu(allwefantasy@gmail.com)
  */
class SelectGrammarAdaptor(grammarProcessListener: GrammarProcessListener) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval)

    val wowText = TemplateMerge.merge(originalText, grammarProcessListener.sqel.env().toMap)

    val chunks = wowText.split("\\s+")
    val tableName = chunks.last.replace(";", "")
    val sql = wowText.replaceAll(s"((?i)as)[\\s|\\n]+${tableName}", "")
    val spark = grammarProcessListener.sqel.sparkSession
    val parser = spark.sessionState.sqlParser
    parser.parsePlan(sql)
  }
}
