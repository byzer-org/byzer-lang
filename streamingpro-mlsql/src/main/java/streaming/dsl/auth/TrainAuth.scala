package streaming.dsl.auth

import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge
import streaming.log.{Logging, WowLog}
import tech.mlsql.dsl.adaptor.DslTool
import tech.mlsql.dsl.processor.AuthProcessListener

/**
  * 2019-05-09 WilliamZhu(allwefantasy@gmail.com)
  */
class TrainAuth(authProcessListener: AuthProcessListener) extends MLSQLAuth with DslTool with Logging with WowLog {
  val env = authProcessListener.listener.env().toMap

  def evaluate(value: String) = {
    TemplateMerge.merge(value, authProcessListener.listener.env().toMap)
  }

  override def auth(_ctx: Any): TableAuthResult = {
    val ctx = _ctx.asInstanceOf[SqlContext]
    var tableName = ""
    var asTableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          tableName = evaluate(s.getText)
        case s: AsTableNameContext =>
          asTableName = evaluate(cleanStr(s.tableName().getText))
        case _ =>
      }
    }

    def isTempTable(name: String) = {
      authProcessListener.listener.sparkSession.catalog.tableExists(name) ||
        authProcessListener.withoutDBs.filter(m => cleanStr(name) == m.table.get).size > 0
    }

    if (isTempTable(tableName)) {
      authProcessListener.addTable(MLSQLTable(None, Some(tableName), None, OperateType.SELECT, None, TableType.TEMP))
    } else {
      authProcessListener.addTable(MLSQLTable(Some("default"), Some(tableName), None, OperateType.SELECT, None, TableType.HIVE))
    }

    if (!asTableName.isEmpty && !isTempTable(asTableName)) {
      authProcessListener.addTable(MLSQLTable(None, Some(asTableName), None, OperateType.LOAD, None, TableType.TEMP))
    }

    TableAuthResult.empty()
  }

}
