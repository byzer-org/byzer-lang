package tech.mlsql.dsl.adaptor

import com.alibaba.druid.sql.repository.SchemaRepository
import com.alibaba.druid.util.JdbcConstants
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.parser.DSLSQLParser
import streaming.dsl.template.TemplateMerge
import tech.mlsql.dsl.processor.GrammarProcessListener

class LoadGrammarAdaptor(grammarProcessListener: GrammarProcessListener) extends DslAdaptor {
  def evaluate(value: String) = {
    TemplateMerge.merge(value, grammarProcessListener.sqel.env().toMap)
  }
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    val loadStatement = new LoadAdaptor(grammarProcessListener).analyze(ctx)
    if(loadStatement.format.toUpperCase == "JDBC" && loadStatement.option.getOrElse("directQuery","") != ""){
      val repository = new SchemaRepository(loadStatement.option.getOrElse("dbType",JdbcConstants.MYSQL))
      val directQuery = evaluate(loadStatement.option.getOrElse("directQuery",""))
      try {
        repository.console(directQuery)
      } catch {
        case e: Exception => {throw new RuntimeException("SQL syntax error, please check:"+directQuery)}
      }
    }
  }
}
