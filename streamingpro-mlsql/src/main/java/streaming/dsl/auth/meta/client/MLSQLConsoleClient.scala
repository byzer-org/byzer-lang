package streaming.dsl.auth.meta.client

import org.apache.http.client.fluent.{Form, Request}
import streaming.common.JSONTool
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.{Logging, WowLog}

/**
  * 2019-03-13 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {

    val context = ScriptSQLExec.contextGetOrForTest()
    val owner = context.owner
    val jsonTables = JSONTool.toJsonStr(tables)
    logDebug(format(jsonTables))
    val authUrl = context.userDefinedParam("__auth_server_url__")
    val auth_secret = context.userDefinedParam("__auth_secret__")
    try {
      val returnJson = Request.Post(authUrl).bodyForm(Form.form().add("tables", jsonTables).
        add("owner", owner).add("home", context.home).add("auth_secret", auth_secret)
        .build())
        .execute().returnContent().asString()
      val res = JSONTool.parseJson[List[Boolean]](returnJson)
      val falseIndex = res.indexOf(false)
      if (falseIndex != -1) {
        val falseTable = tables(falseIndex)
        throw new RuntimeException(
          s"""
             |Error:
             |
             |db:    ${falseTable.db.getOrElse("")}
             |table: ${falseTable.table.getOrElse("")}
             |tableType: ${falseTable.tableType.name}
             |sourceType: ${falseTable.sourceType.getOrElse("")}
             |operateType: ${falseTable.operateType.toString}
             |
             |is not allowed to access.
           """.stripMargin)
      }
    } catch {
      case e: Exception if !e.isInstanceOf[RuntimeException] =>
        throw new RuntimeException("Auth control center maybe down casued by " + e.getMessage)
    }

    List()
  }
}
