package tech.mlsql.dsl.adaptor

import streaming.dsl.DslAdaptor
import streaming.dsl.parser.DSLSQLParser
import tech.mlsql.dsl.processor.PreProcessListener

/**
  * 2019-04-11 WilliamZhu(allwefantasy@gmail.com)
  */
class StatementAdaptor(preProcessListener: PreProcessListener) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    preProcessListener.addStatement(currentText(ctx))
  }
}

class StatementForIncludeAdaptor(preProcessListener: PreProcessIncludeListener) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {
    preProcessListener.addStatement(currentText(ctx),SCType.Normal)
  }
}
