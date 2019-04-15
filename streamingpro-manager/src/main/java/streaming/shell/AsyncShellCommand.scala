/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.shell

import java.io.{OutputStream, RandomAccessFile}
import java.util.concurrent.{Callable, Executors, FutureTask, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import net.csdn.common.logging.Loggers

import scala.concurrent.SyncVar
import scala.io.Source
import scala.sys.process._

/**
  * Created by allwefantasy on 12/7/2017.
  */
class AsyncShellCommand(user: String, command: String, token: String, timeout: Long, isInteractive: Boolean = false) extends TFileWriter {

  val logger = Loggers.getLogger(classOf[AsyncShellCommand])
  val taskId = Md5.MD5(command) + "_" + token
  val homeDir = s"/tmp/mammuthus/${taskId}"
  ShellCommand.exec(s"mkdir -p $homeDir")

  val p = Process(commandResolve)

  //用户交互输入
  val inputStream = new SyncVar[OutputStream]

  val stdout = new java.io.FileWriter(new java.io.File(s"$homeDir/stdout"))
  val stderr = new java.io.FileWriter(new java.io.File(s"$homeDir/stderr"))
  val stdin = new java.io.FileWriter(new java.io.File(s"$homeDir/stdin"))


  val isFinished = new AtomicBoolean(false)
  val exitValue = new AtomicInteger(0)
  val isTimeout = new AtomicBoolean(false)

  val pio = new ProcessIO(
    in => {
      inputStream.put(in)
    },
    out => {
      Source.fromInputStream(out).getLines().foreach {
        f => stdout.write(f + "\n"); stdout.flush()
      }
    },
    err => {
      Source.fromInputStream(err).getLines().foreach {
        f => stderr.write(f + "\n"); stderr.flush()
      }
    })


  val process = p.run(pio)

  selfClose

  /*
     if in interactive mode,selfClose should not been enabled
   */
  private def selfClose: Boolean = {
    if (isInteractive) return false
    val futureTask = new FutureTask[Boolean](new Callable[Boolean] {
      override def call(): Boolean = {
        exitValue.set(process.exitValue())
        isFinished.set(true)
        close()
        logger.info(s"task $taskId close itself with exit value ${exitValue.get()}")
        true
      }
    })
    AsyncShellCommand.schedule.execute(futureTask)

    AsyncShellCommand.schedule.execute(new Runnable {
      override def run(): Unit = {
        try futureTask.get(timeout, TimeUnit.MILLISECONDS) catch {
          case e: Exception =>
            isFinished.set(true)
            isTimeout.set(true)
            exitValue.set(-1)
            close()
            logger.info(s"task $taskId execute timeout (${timeout}s). system close it}")
        }
      }
    })

    return true
  }

  def commandResolve = {
    if (isInteractive) command
    else commandToFile
  }

  def findTaskId = taskId

  def commandToFile = {
    val fileName = taskId + ".sh"
    writeToFile(homeDir + "/" + fileName, "#!/bin/bash\nsource /etc/profile\n" + command)
    s"chmod 777 $homeDir/$fileName".!
    val commandWrapped = s"$homeDir/$fileName"
    if(user!=null && !user.isEmpty){
      s"su - $user -c '$commandWrapped'"
    }else commandWrapped
  }

  def execute(line: String) = {
    inputStream.get.write((line + "\n").getBytes())
    inputStream.get.flush()

  }


  def progress(offset: Long, readSize: Long): (Long, String) = {
    var fr = new RandomAccessFile(s"$homeDir/stderr", "r")
    fr = if (fr.length() > 0) fr
    else {
      fr.close()
      new RandomAccessFile(s"$homeDir/stdout", "r")
    }
    try {
      val localOffset = offset match {
        case x if x < 0 && fr.length() >= 10000 => 10000
        case x if x < 0 && fr.length() < 10000 => 0
        case x if x >= 0 => offset
      }
      fr.seek(localOffset)

      val bytes = new Array[Byte](readSize.toInt)
      val len = fr.read(bytes)
      if (len > 0)
        (fr.length(), new String(bytes, 0, len, "UTF-8"))
      else
        (0, "")
    } finally fr.close()

  }

  def progress(offset: Long): (Long, String) = {
    return progress(offset, 1024 * 100)
  }

  def removeTempFile = {
    if (!isInteractive) {
      logger.info(s"task $taskId is self closing phase: clean up temp directory")
      s"rm -rf ${homeDir}" !
    }
  }

  def close() = {
    if (inputStream.isSet) {
      inputStream.get.close()
      logger.info(s"task $taskId is self closing phase: close inputStream")
    }
    stdout.close()
    logger.info(s"task $taskId is self closing phase: close stdout")
    stderr.close()
    logger.info(s"task $taskId is self closing phase: close stderr")
    process.destroy()
    logger.info(s"task $taskId is self closing phase: process destroy")
  }

  def closeWithExitCommand(exitCommand: String) = {
    execute(exitCommand)
    close
  }
}

object AsyncShellCommand {

  val logger = Loggers.getLogger(classOf[AsyncShellCommand])
  //暂时没有对任务做持久化
  val shellsInfo = new scala.collection.mutable.HashMap[String, (AsyncShellCommand, Long)]()
  val removeList = new scala.collection.mutable.ArrayBuffer[String]()
  //如果一个终端或者进程一直没有被使用，则关闭它
  val schedule = Executors.newScheduledThreadPool(1000)
  val maxTimeIdle = 1000 * 60 * 60 * 3
  //默认三个小时
  val defaultTimeOut = 300 * 1000l
  schedule.schedule(new Runnable {
    override def run(): Unit = {
      try {
        logger.info(s"schedule to clean up tasks expired without any operation in 3 hours")
        shellsInfo.filter {
          f =>
            (System.currentTimeMillis() - f._2._2) > maxTimeIdle
        }.map(f => f._1).foreach {
          f =>
            logger.info(s"clean up task $f cause this task without any operation in 3 hours")
            shellsInfo.remove(f) match {
              case Some(i) =>
                i._1.close()
                i._1.removeTempFile

              case None =>
            }
        }
      } catch {
        case e: Exception =>
          logger.error("schedule to clean up tasks expired without any operation in 3 hours failed,please check error log", e)
      }
    }

  }, 10, TimeUnit.SECONDS)


  def start(command: String, token: String, isInteractive: Boolean = false): String = {
    return startWithTimeout(command, token, defaultTimeOut, isInteractive)
  }

  def startWithTimeout(command: String, token: String, timeout: Long, isInteractive: Boolean = false) = {
    startWithUserAndTimeout("",command,token,timeout,isInteractive)
  }

  def startWithUserAndTimeout(user:String,command: String, token: String, timeout: Long, isInteractive: Boolean = false) = {
    if (isInteractive) {
      logger.info(s"initial interactive shell $command with token $token")
    }
    val shellCommand = new AsyncShellCommand(user,command, token, timeout, isInteractive)
    shellsInfo += (shellCommand.taskId ->(shellCommand, System.currentTimeMillis()))
    shellCommand.taskId
  }


  def progress(taskId: String, offset: Long = 0) = {
    if (!shellsInfo.contains(taskId)) {
      (ShellExecuteStatus(false, false, false), (-1l, "no message"))
    }
    else {
      val isc = shellsInfo(taskId)._1
      (ShellExecuteStatus(isc.isFinished.get(), isc.isTimeout.get(), isc.exitValue.get() != 0), shellsInfo(taskId)._1.progress(offset))
    }
  }

  def execute(taskId: String, line: String) = {
    logger.info(s"task $taskId in interactive mode. execute input $line")
    try shellsInfo(taskId)._1.execute(line)
    finally shellsInfo += (taskId ->(shellsInfo(taskId)._1, System.currentTimeMillis()))
  }

  def close(taskId: String) = {
    if (shellsInfo.contains(taskId)) {
      shellsInfo(taskId)._1.close()
    }
  }

  def close(taskId: String, command: String) = {
    if (shellsInfo.contains(taskId)) {
      shellsInfo(taskId)._1.closeWithExitCommand(command)
    }
  }

}

case class ShellExecuteStatus(finished: Boolean, isTimeout: Boolean, isError: Boolean)
