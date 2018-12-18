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

package streaming.common

/**
  * Created by allwefantasy on 29/3/2018.
  */

import java.io.{File, FileWriter, RandomAccessFile}

import net.csdn.common.logging.Loggers
import streaming.common.shell.AsyncShellCommand

import scala.sys.process.ProcessLogger
import scala.sys.process._


/**
  * Created by allwefantasy on 12/7/2017.
  */
object ShellCommand extends TFileWriter {
  val logger = Loggers.getLogger(classOf[ShellCommand])

  def exec(shellStr: String): String = {
    try {
      exec("", shellStr)
    } catch {
      case e: Exception =>
        logger.info("exec fail", e)
        ""
    }
  }

  def wrapCommand(user: String, fileName: String) = {
    if (user != null && !user.isEmpty) {
      s"su - $user /bin/bash -c '/bin/bash /tmp/$fileName'"
    } else s"/bin/bash /tmp/$fileName"
  }

  def readFile(dir: String, offset: Long, readSize: Long): (Long, String) = {
    var fr = new RandomAccessFile(s"$dir/stderr", "r")
    fr = if (fr.length() > 0) fr
    else {
      fr.close()
      new RandomAccessFile(s"$dir/stdout", "r")
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


  def exec(user: String, shellStr: String) = {
    logger.debug("shell " + shellStr)
    val fileName = System.currentTimeMillis() + "_" + Math.random() + ".sh"
    writeToFile("/tmp/" + fileName, "#!/bin/bash\nsource /etc/profile\n" + shellStr)
    s"chmod u+x /tmp/$fileName".!
    val result = wrapCommand(user, fileName).!!.trim
    s"rm /tmp/$fileName".!
    result
  }

  def execFile(fileName: String) = {
    logger.debug("exec file " + fileName)

    s"chmod u+x ${fileName}".!
    val result = s"/bin/bash $fileName".!!.trim
    result
  }

  /*
    todo: timeout supported
   */
  def execWithExitValue(shellStr: String, timeout: Long = AsyncShellCommand.defaultTimeOut): (Int, String, String) = {
    execWithUserAndExitValue("", shellStr, timeout)
  }

  def execSimpleCommand(shellStr: String) = {
    shellStr !!
  }

  def execWithUserAndExitValue(user: String, shellStr: String, timeout: Long = AsyncShellCommand.defaultTimeOut) = {
    val out = new StringBuilder
    val err = new StringBuilder
    val et = ProcessLogger(
      line => out.append(line + "\n"),
      line => err.append(line + "\n"))
    logger.debug("shell " + shellStr)
    val fileName = System.currentTimeMillis() + "_" + Math.random() + ".sh"
    writeToFile("/tmp/" + fileName, "#!/bin/bash\nsource /etc/profile\n" + shellStr)
    s"chmod u+x /tmp/$fileName".!
    val pb = Process(wrapCommand(user, fileName))
    val exitValue = pb ! et
    s"rm /tmp/$fileName".!
    logger.debug(s"rm /tmp/$fileName")
    (exitValue, err.toString().trim, out.toString().trim)
  }


  def progress(filePath: String, offset: Long, size: Int): (Long, String, Long) = {
    if (!new File(filePath).exists()) return (-1, "", 0)

    val fr = new RandomAccessFile(filePath, "r")
    if (fr.length() <= offset) {
      fr.close()
      return (-1, "", 0)
    }
    try {
      val temp = 4 * 1024 //if (fr.length() > 4 * 1024) 4 * 1024 else 0
      var newSize: Long = if (size <= 0 || size > 1024 * 1024) temp else size
      val newOffset = if (offset == -1 && fr.length() > temp) fr.length() - temp else if (offset == -1) 0 else offset
      fr.seek(newOffset)
      if (newOffset + newSize >= fr.length()) newSize = fr.length() - newOffset
      val bytes = new Array[Byte](newSize.toInt)
      fr.readFully(bytes)
      (fr.getFilePointer, new String(bytes, "UTF-8"), fr.length())
    } finally fr.close()

  }


}

class ShellCommand

trait TFileWriter {
  def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }

  def writeToFile(fileName: String, data: String) =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.append(data)
    }
}
