package streaming.shell

import java.io.{File, FileWriter, RandomAccessFile}

import net.csdn.common.logging.Loggers

import scala.sys.process.ProcessLogger
import scala.sys.process._

/**
  * Created by allwefantasy on 12/7/2017.
  */
object ShellCommand extends TFileWriter {
  val logger = Loggers.getLogger(classOf[ShellCommand])

  def exec(shellStr: String): String = {
    exec("", shellStr)
  }

  def wrapCommand(user: String, fileName: String) = {
    if (user != null && !user.isEmpty) {
      s"su - $user /bin/bash -c '/bin/bash /tmp/$fileName'"
    } else s"/bin/bash /tmp/$fileName"
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
