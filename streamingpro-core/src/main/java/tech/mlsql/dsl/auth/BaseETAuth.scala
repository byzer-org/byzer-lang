package tech.mlsql.dsl.auth

import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth._
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

/**
 * 13/10/2021 WilliamZhu(allwefantasy@gmail.com)
 */
trait BaseETAuth extends ETAuth {
  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option(etName),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }

  def etName: String
}
