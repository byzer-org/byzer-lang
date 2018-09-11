package streaming.dsl.auth.meta.client

import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.{Logging, WowLog}

/**
  * Created by allwefantasy on 11/9/2018.
  */
class DefaultConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {
    logInfo(format(s"auth ${tables.mkString(",")}"))
    throw new RuntimeException("auth fail")
  }
}
