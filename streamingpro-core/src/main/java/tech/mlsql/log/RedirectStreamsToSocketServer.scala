package tech.mlsql.log

import java.io.{DataOutputStream, InputStream}
import java.net.Socket

import tech.mlsql.arrow.Utils
import tech.mlsql.arrow.api.RedirectStreams
import tech.mlsql.arrow.python.runner.PythonConf
import tech.mlsql.common.utils.lang.sc.ScalaMethodMacros
import tech.mlsql.common.utils.log.Logging

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
  WriteLog.init(conf)

  override def run() {
    WriteLog.write(scala.io.Source.fromInputStream(in).getLines(), conf)
  }
}

class WriteLogPool(size: Int, conf: Map[String, String]) {
  val pool = new java.util.concurrent.ConcurrentLinkedQueue[WriteLog]()

  def init(): Unit = {
    (0 until size).foreach { item =>
      pool.add(createObj())
    }
  }

  def borrowObj() = {
    pool.poll()
  }

  def returnObj(writeLog: WriteLog) = {
    pool.offer(writeLog)
  }

  private def createObj() = {
    new WriteLog(conf)
  }

}

class WriteLog(conf: Map[String, String]) extends Logging {

  val host = conf("spark.mlsql.log.driver.host")
  val port = conf("spark.mlsql.log.driver.port")
  val token = conf("spark.mlsql.log.driver.token")

  logInfo(s"Init WriteLog in executor. The target DriverLogServer is ${host}:${port} with token ${token}")
  val socket = new Socket(host, port.toInt)

  def write(in: Iterator[String], params: Map[String, String]) = {
    val dOut = new DataOutputStream(socket.getOutputStream)
    in.foreach { line =>
      val client = new DriverLogClient()
      val owner = params.getOrElse(ScalaMethodMacros.str(PythonConf.PY_EXECUTE_USER), "")
      val groupId = params.getOrElse("groupId", "")
      client.sendRequest(dOut, SendLog(token, LogUtils.formatWithOwner(line, owner, groupId)))
    }
  }

  def close = {
    socket.close()
  }
}

object WriteLog {

  var POOL: WriteLogPool = null

  def init(conf: Map[String, String]) = {
    synchronized {
      if (POOL == null) {
        POOL = new WriteLogPool(30, conf)
        POOL.init()
      }
    }

  }

  def write(in: Iterator[String], params: Map[String, String]): Unit = {
    if (POOL == null) {
      in.foreach(println(_))
      return
      //throw new RuntimeException("WriteLog Pool is not init")
    }
    val obj = POOL.borrowObj()
    Utils.tryWithSafeFinally {
      if (obj == null) {
        throw new RuntimeException("Cannot get connection for task writing log to driver")
      }
      obj.write(in, params)

    } {
      if (obj != null) {
        POOL.returnObj(obj)
      }
    }
  }
}
