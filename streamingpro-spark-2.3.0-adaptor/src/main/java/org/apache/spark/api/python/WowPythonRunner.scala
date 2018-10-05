package org.apache.spark.api.python

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util
import java.util.{List => JList, Map => JMap}

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.{InterruptibleIterator, MLSQLPythonEnv, SparkEnv, SparkException, SparkFiles, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


/**
  * Created by allwefantasy on 5/2/2018.
  */
object WowPythonRunner extends Logging {
  val bufferSize = 65536
  val reuse_worker = true


  // kafkaParam: Map[String, String]
  def run(daemonCommand: Option[Seq[String]],
          workerCommand: Option[Seq[String]],
          idleWorkerTimeoutMS: Long,
          pythonVer: String,
          command: Array[Byte],
          inputIterator: Iterator[_],
          partitionIndex: Int, model: Array[Byte],
          runtimeParams: Map[Any, Any],
          recordLog: Any => Any) = {
    val envVars: JMap[String, String] = new util.HashMap[String, String]()
    val pythonIncludes = new util.ArrayList[String]()
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val deployAPI = runtimeParams.getOrElse("streaming.deploy.rest.api", "false").toString.toBoolean
    val mlsqlEnv = new MLSQLPythonEnv(env, deployAPI)

    val worker: Socket = mlsqlEnv.createPythonWorker(daemonCommand, workerCommand, envVars.asScala.toMap, (msg) => {
      recordLog(msg)
    }, idleWorkerTimeoutMS)

    // Whether is the worker released into idle pool
    @volatile var released = false
    val context = TaskContext.get()

    def _run(): Unit = Utils.logUncaughtExceptions {
      try {
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(pythonVer, dataOut)
        // Write out the TaskContextInfo
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.size)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        dataOut.writeInt(0)
        dataOut.flush()
        // Serialized command:
        dataOut.writeInt(0)
        dataOut.writeInt(command.length)
        dataOut.write(command)
        // Data values
        PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logInfo("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          recordLog(e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      }
    }

    _run()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = new Iterator[Array[Byte]] {
      override def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        try {

          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.TIMING_DATA =>
              // Timing data from worker
              val bootTime = stream.readLong()
              val initTime = stream.readLong()
              val finishTime = stream.readLong()
              val boot = bootTime - startTime
              val init = initTime - bootTime
              val finish = finishTime - initTime
              val total = finishTime - startTime
              recordLog("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
                init, finish))
              val memoryBytesSpilled = stream.readLong()
              val diskBytesSpilled = stream.readLong()
              context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
              context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              val msg = new String(obj, StandardCharsets.UTF_8)
              recordLog(msg)
              throw new PythonException(msg,
                null)
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
              }
              // Check whether the worker is ready to be re-used.
              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
                if (reuse_worker) {
                  mlsqlEnv.releasePythonWorker(daemonCommand, workerCommand, envVars.asScala.toMap, worker)
                  released = true
                }
              }
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            recordLog(e)
            throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

          case e: Exception if env.isStopped =>
            logDebug("Exception thrown after context is stopped", e)
            null // exit silently

          case eof: EOFException =>
            recordLog(eof)
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      override def hasNext: Boolean = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
  }

}


/** Thrown for exceptions in user Python code. */
private class PythonException(msg: String, cause: Exception) extends RuntimeException(msg, cause)
