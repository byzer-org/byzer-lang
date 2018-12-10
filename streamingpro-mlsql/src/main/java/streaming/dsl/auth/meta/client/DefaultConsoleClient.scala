package streaming.dsl.auth.meta.client

import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.{Logging, WowLog}

/**
  * Created by allwefantasy on 11/9/2018.
  */
class DefaultConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    logInfo(format(s"auth ${owner}  want access tables: ${tables.mkString(",")}"))
    if (owner == "william"){
      throw new RuntimeException("auth fail")
    }else{
      List(TableAuthResult(true ,""))
    }
  }
}
