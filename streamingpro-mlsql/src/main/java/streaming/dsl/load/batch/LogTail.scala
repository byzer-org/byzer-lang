package streaming.dsl.load.batch

import java.util
import java.util.regex.Pattern

import streaming.common.shell.ShellCommand
import tech.mlsql.MLSQLEnvKey

import scala.collection.JavaConverters._


/**
  * 2019-04-02 WilliamZhu(allwefantasy@gmail.com)
  */
object LogTail {
  private val pattern = Pattern.compile("\\d{2}/\\d{2}/\\d{2}\\s\\d{2}\\:\\d{2}\\:\\d{2}\\s")

  def log(owner: String, _filePath: String, offset: Long, size: Int = 1024 * 1024 - 1) = {
    val filePath = s"${MLSQLEnvKey.realTimeLogHome}/mlsql_engine.log"
    val (newOffset, msg, fileSize) = ShellCommand.progress(filePath, offset, size)
    val (lines, backoff) = merge(msg)
    val newMsg = lines.asScala.filter(f => (f.contains(s"[owner] [${owner}]") || f.contains("DistriOptimizer$: [")) && !f.contains("load _mlsql_.`"))
    LogMsg(newOffset - backoff, newMsg)
  }

  private def merge(message: String) = {
    val mat = pattern.matcher(message)
    var list = new util.ArrayList[Position]()
    while (mat.find()) {
      val start = mat.start()
      val end = mat.end()
      list.add(Position(start, end))
    }
    val size = list.size()
    val lastP = list.get(size - 1)
    val backoff = message.substring(lastP.start).getBytes("UTF-8").length
    val toSend = message.substring(0, lastP.start - 1)
    list.remove(size - 1)
    var lines = new util.ArrayList[String]()
    var i = 0
    while (i < list.size() - 1) {
      var log = toSend.substring(list.get(i).start, list.get(i + 1).start - 1)
      i += 1
      lines.add(log)
    }
    lines.add(toSend.substring(list.get(list.size() - 1).start))
    (lines, backoff)
  }
}

case class LogMsg(offset: Long, msg: Seq[String])

case class Position(start: Int, end: Int)




