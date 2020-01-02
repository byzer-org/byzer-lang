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
    var option = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case _ =>
      }
    }
    val dbType = option.getOrElse("dbType",JdbcConstants.MYSQL)
    val directQuery = option.getOrElse("directQuery","select a from b")
    val repository = new SchemaRepository(dbType)
    try {
      repository.console(directQuery)
    } catch {
      case e: Exception => {throw new RuntimeException("SQL syntax error, please check:"+directQuery)}
    }
  }

}
