package streaming.dsl.load.batch

import streaming.common.shell.ShellCommand
import tech.mlsql.MLSQLEnvKey


/**
  * 2019-04-02 WilliamZhu(allwefantasy@gmail.com)
  */
object LogTail {

  def log(owner: String, _filePath: String, offset: Long, size: Int = 1024 * 1024 - 1) = {
    val filePath = s"${MLSQLEnvKey.realTimeLogHome}/mlsql_engine.log"
    val (newOffset, msg, fileSize) = ShellCommand.progress(filePath, offset, size)
    val newMsg = msg.split("\n").filter(f => (f.contains(s"[owner] [${owner}]") || f.contains("DistriOptimizer$: [")) && !f.contains("load _mlsql_.`"))
    LogMsg(newOffset, newMsg)
  }
}

case class LogMsg(offset: Long, msg: Seq[String])




