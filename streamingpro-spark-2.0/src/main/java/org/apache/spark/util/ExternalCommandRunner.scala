package org.apache.spark.util

import java.io._

import java.util.concurrent.atomic.AtomicReference
import net.csdn.common.logging.Loggers
import org.apache.spark.sql.types._
import org.apache.spark.{SparkEnv}
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.io.Source
import ObjPickle._

/**
  * Created by allwefantasy on 24/1/2018.
  */
object ExternalCommandRunner {
  val logger = Loggers.getLogger(classOf[ExternalCommandRunner])

  def run(command: Seq[String],
          iter: Any,
          schema: DataType,
          scriptContent: String,
          scriptName: String,
          modelPath: String,
          envVars: Map[String, String] = Map(),
          separateWorkingDir: Boolean = true,
          bufferSize: Int = 1024,
          encoding: String = "utf-8") = {

    val pb = new ProcessBuilder(command.asJava)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    logger.debug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      logger.debug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        //        for (file <- currentDir.list(tasksDirFilter)) {
        //          val fileWithDir = new File(currentDir, file)
        //          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
        //            new File(taskDirectory + File.separator + fileWithDir.getName()))
        //        }
        pb.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception => logger.error("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e)
      }
    }

    //使用pickle 把数据写到work目录，之后python程序会读取
    val fileTemp = new File(taskDirectory + "/python_temp.pickle")
    currentEnvVars.put("pickleFile", fileTemp.getPath)
    val pythonTempFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileTemp)))
    // writeIteratorToStream(ExternalCommandRunner.pickle(iter, schema), pythonTempFile)
    pickle(iter, pythonTempFile)

    def saveFile(scriptName: String, scriptContent: String) = {
      val scriptFile = new File(taskDirectory + s"/${scriptName}")
      val fw = new FileWriter(scriptFile)
      try {
        fw.write(scriptContent)
      } finally {
        fw.close()
      }
    }
    saveFile(scriptName, scriptContent)

    def savePythonFile(name: String) = {
      val msg_queue = Source.fromInputStream(ExternalCommandRunner.getClass.getResourceAsStream("/python/" + name)).
        getLines().mkString("\n")
      saveFile(name, msg_queue)
    }

    savePythonFile("msg_queue.py")
    savePythonFile("mlsql.py")
    savePythonFile("mlsql_model.py")
    savePythonFile("mlsql_tf.py")
    savePythonFile("python_fun.py")

    val proc = pb.start()
    val env = SparkEnv.get
    val childThreadException = new AtomicReference[Throwable](null)

    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $command") {
      override def run(): Unit = {
        val err = proc.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(encoding).getLines) {
            // scalastyle:off println
            System.err.println(line)
            // scalastyle:on println
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread(s"stdin writer for $command") {
      override def run(): Unit = {
        val out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(proc.getOutputStream, encoding), bufferSize))
        try {
          // scalastyle:off println
          // out.println()
          // scalastyle:on println
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines
    new Iterator[String] {
      def next(): String = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }
        val line = lines.next()
        println(line)
        line
      }

      def hasNext(): Boolean = {
        val result = if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          cleanup()
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        propagateChildException()
        result
      }

      private def cleanup(): Unit = {
        // cleanup task working directory if used
        if (workInTaskDirectory) {
          scala.util.control.Exception.ignoring(classOf[IOException]) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
          logger.debug(s"Removed task working directory $taskDirectory")
        }
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          val commandRan = command.mkString(" ")
          logger.error(s"Caught exception while running pipe() operator. Command ran: $commandRan. " +
            s"Exception: ${t.getMessage}")
          proc.destroy()
          cleanup()
          throw t
        }
      }
    }

  }

}

class ExternalCommandRunner

class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
  def accept(dir: File, name: String): Boolean = {
    !name.equals(filterName)
  }
}