package tech.mlsql.log

import java.io.{DataOutputStream, IOException, InputStream}
import java.net.Socket

import tech.mlsql.arrow.Utils
import tech.mlsql.arrow.api.RedirectStreams
import tech.mlsql.arrow.python.runner.PythonConf
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros

/**
  * 2019-08-21 WilliamZhu(allwefantasy@gmail.com)
  */
class RedirectStreamsToSocketServer extends RedirectStreams {
  var _conf: Map[String, String] = null

  override def setConf(conf: Map[String, String]): Unit = _conf = conf

  override def conf: Map[String, String] = _conf

  override def stdOut(stdOut: InputStream): Unit = {
    new RedirectLogThread(stdOut, conf, "stdout reader for logger server").start()
  }

  override def stdErr(stdErr: InputStream): Unit = {
    new RedirectLogThread(stdErr, conf, "stderr reader for logger server").start()
  }


}

class RedirectLogThread(
                         in: InputStream,
                         conf: Map[String, String],
                         name: String,
                         propagateEof: Boolean = false)
  extends Thread(name) {

  setDaemon(true)

  override def run() {
    WriteLog.write(scala.io.Source.fromInputStream(in).getLines(), conf, propagateEof)
  }
}

object WriteLog {
  def write(in: Iterator[String], conf: Map[String, String], propagateEof: Boolean = false) = {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      val host = conf("spark.mlsql.log.driver.host")
      val port = conf("spark.mlsql.log.driver.port")
      val token = conf("spark.mlsql.log.driver.token")
      val owner = conf.getOrElse(ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER), "")
      val groupId = conf.getOrElse("groupId", "")
      val socket = new Socket(host, port.toInt)

      Utils.tryWithSafeFinally {
        val dOut = new DataOutputStream(socket.getOutputStream)
        in.foreach { line =>
          val client = new DriverLogClient()
          client.sendRequest(dOut, SendLog(token, LogUtils.formatWithOwner(line, owner, groupId)))
        }

      } {
        if (propagateEof) {
          socket.close()
        }
      }
    }
  }
}
