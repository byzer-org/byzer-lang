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

package org.apache.spark.api.python

import java.io._
import java.net._
import java.util
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.util._


/**
  * Enumerate the type of command that will be sent to the Python worker
  */
private[spark] object WowPythonEvalType {
  val NON_UDF = 0

  val SQL_BATCHED_UDF = 100

  val SQL_SCALAR_PANDAS_UDF = 200
  val SQL_GROUPED_MAP_PANDAS_UDF = 201

  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case NON_UDF => "NON_UDF"
    case SQL_BATCHED_UDF => "SQL_BATCHED_UDF"
    case SQL_SCALAR_PANDAS_UDF => "SQL_SCALAR_PANDAS_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF => "SQL_GROUPED_MAP_PANDAS_UDF"
  }
}

/**
  * A helper class to run Python mapPartition/UDFs in Spark.
  *
  * funcs is a list of independent Python functions, each one of them is a list of chained Python
  * functions (from bottom to top).
  */
private[spark] abstract class WowBasePythonRunner[IN, OUT](
                                                            daemonCommand: Option[Seq[String]],
                                                            workerCommand: Option[Seq[String]],
                                                            execCommand: Array[Byte],
                                                            envVars: util.Map[String, String],
                                                            bufferSize: Int,
                                                            reuseWorker: Boolean,
                                                            evalType: Int,
                                                            argOffsets: Array[Array[Int]],
                                                            recordLog: String => Unit,
                                                            idleWorkerTimeoutMS: Long
                                                          )
  extends Logging {

  def uuid = {
    s"${daemonCommand.get.mkString("-")}-${workerCommand.get.mkString("-")}"
  }

  /**
    * WowPythonRunner used in api deploy mode. This mode only support spark local mode.
    * In consideration of performance, we will not invoke task scheduler and run driver thread directly.
    * Hence there is no any "TaskContext" concept here.
    *
    **/
  def run(
           inputIterator: Iterator[IN],
           partitionIndex: Int,
           context: TaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    val mlsqlEnv = new MLSQLPythonEnv(SparkEnv.get, true)
    val localdir = mlsqlEnv.sparkEnv.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    //noCache is for debug
    val worker: Socket = mlsqlEnv.createPythonWorker(
      daemonCommand,
      workerCommand,
      envVars.asScala.toMap, (msg) => {
        recordLog(msg)
      }, idleWorkerTimeoutMS, true
    )
    // Whether is the worker released into idle pool
    val released = new AtomicBoolean(false)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = newWriterThread(mlsqlEnv, worker, inputIterator, partitionIndex, context)

    writerThread.start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = newReaderIterator(
      stream, writerThread, startTime, mlsqlEnv, worker, released, context)
    // make sure command have be invoked in python worker
    //    writerThread.join()
    new InterruptibleIterator(context, stdoutIterator)
  }

  protected def newWriterThread(
                                 env: MLSQLPythonEnv,
                                 worker: Socket,
                                 inputIterator: Iterator[IN],
                                 partitionIndex: Int,
                                 context: TaskContext): WriterThread

  protected def newReaderIterator(
                                   stream: DataInputStream,
                                   writerThread: WriterThread,
                                   startTime: Long,
                                   env: MLSQLPythonEnv,
                                   worker: Socket,
                                   released: AtomicBoolean,
                                   context: TaskContext): Iterator[OUT]

  /**
    * The thread responsible for writing the data from the PythonRDD's parent iterator to the
    * Python process.
    */
  abstract class WriterThread(
                               env: MLSQLPythonEnv,
                               worker: Socket,
                               inputIterator: Iterator[IN],
                               partitionIndex: Int,
                               context: TaskContext)
    extends Thread(s"stdout writer for  $uuid") {

    @volatile private var _exception: Exception = null

    /** here, we do not need pythonIncludes/broadcastVars, just make sure they are empty. */
    private val pythonIncludes = Set()
    private val broadcastVars = Seq[Broadcast[PythonBroadcast]]()

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      this.interrupt()
    }

    /**
      * Writes a command section to the stream connected to the Python worker.
      */
    protected def writeCommand(dataOut: DataOutputStream): Unit

    /**
      * Writes input data to the stream connected to the Python worker.
      */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(envVars.getOrDefault("pythonVer", "3.6"), dataOut)
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
        // Broadcast variables
        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        val toRemove = oldBids.diff(newBids)
        val cnt = toRemove.size + newBids.diff(oldBids).size
        dataOut.writeInt(cnt)
        for (bid <- toRemove) {
          // remove the broadcast from worker
          dataOut.writeLong(-bid - 1) // bid >= 0
          oldBids.remove(bid)
        }
        for (broadcast <- broadcastVars) {
          if (!oldBids.contains(broadcast.id)) {
            // send new broadcast
            dataOut.writeLong(broadcast.id)
            PythonRDD.writeUTF(broadcast.value.path, dataOut)
            oldBids.add(broadcast.id)
          }
        }
        dataOut.flush()

        dataOut.writeInt(evalType)
        writeCommand(dataOut)
        writeIteratorToStream(dataOut)

        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          logError("Exception thrown when write command to python worker:", e)
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
      }
    }
  }

  abstract class ReaderIterator(
                                 stream: DataInputStream,
                                 writerThread: WriterThread,
                                 startTime: Long,
                                 env: MLSQLPythonEnv,
                                 worker: Socket,
                                 released: AtomicBoolean,
                                 context: TaskContext)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    private var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
      * Reads next object from the stream.
      * When the stream reaches end of data, needs to process the following sections,
      * and then returns null.
      */
    protected def read(): OUT

    protected def handleTimingData(): Unit = {
      // Timing data from worker
      val bootTime = stream.readLong()
      val initTime = stream.readLong()
      val finishTime = stream.readLong()
      val boot = bootTime - startTime
      val init = initTime - bootTime
      val finish = finishTime - initTime
      val total = finishTime - startTime
      logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
        init, finish))
      val memoryBytesSpilled = stream.readLong()
      val diskBytesSpilled = stream.readLong()
      context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
      context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
    }

    protected def handlePythonException(): PythonException = {
      // Signals that an exception has been thrown in python
      val exLength = stream.readInt()
      val obj = new Array[Byte](exLength)
      stream.readFully(obj)
      new PythonException(new String(obj, StandardCharsets.UTF_8),
        writerThread.exception.getOrElse(null))
    }

    protected def handleEndOfDataSection(): Unit = {
      // We've finished the data section of the output, but we can still
      // read some accumulator updates.
      // Since WowPythonRunner is for model predict, we should make sure
      // we consume the accumulator updates, actually, this should be zero.
      stream.readInt()

      // Check whether the worker is ready to be re-used.
      if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
        if (reuseWorker) {
          env.releasePythonWorker(daemonCommand, workerCommand, envVars.asScala.toMap, worker)
          released.set(true)
        }
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception if writerThread.exception.isDefined =>
        logError("Python worker exited unexpectedly (crashed)", e)
        logError("This may have been caused by a prior exception:", writerThread.exception.get)
        throw writerThread.exception.get

      case eof: EOFException =>
        throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
      case e: Exception =>
        throw new SparkException("unknown reason", e)
    }
  }


}


/**
  * A helper class to run Python mapPartition in Spark.
  */
private[spark] class WowPythonRunner(
                                      daemonCommand: Option[Seq[String]],
                                      workerCommand: Option[Seq[String]],
                                      execCommand: Array[Byte],
                                      envVars: util.Map[String, String],
                                      bufferSize: Int,
                                      reuseWorker: Boolean,
                                      recordLog: String => Unit,
                                      idleWorkerTimeoutMS: Long)
  extends WowBasePythonRunner[Array[Byte], Array[Byte]](
    daemonCommand, workerCommand, execCommand, envVars, bufferSize, reuseWorker, WowPythonEvalType.NON_UDF, Array(Array(0)), recordLog, idleWorkerTimeoutMS
  ) {

  protected override def newWriterThread(
                                          env: MLSQLPythonEnv,
                                          worker: Socket,
                                          inputIterator: Iterator[Array[Byte]],
                                          partitionIndex: Int,
                                          context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        dataOut.writeInt(execCommand.length)
        dataOut.write(execCommand)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      }
    }
  }

  protected override def newReaderIterator(
                                            stream: DataInputStream,
                                            writerThread: WriterThread,
                                            startTime: Long,
                                            env: MLSQLPythonEnv,
                                            worker: Socket,
                                            released: AtomicBoolean,
                                            context: TaskContext): Iterator[Array[Byte]] = {
    new ReaderIterator(stream, writerThread, startTime, env, worker, released, context) {

      protected override def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case WowSpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case WowSpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case WowSpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}

object WowPythonRunner {
  def runner(daemonCommand: Option[Seq[String]],
             workerCommand: Option[Seq[String]],
             execCommand: Array[Byte],
             envVars: util.Map[String, String],
             reuseWorker: Boolean,
             recordLog: String => Unit,
             idleWorkerTimeoutMS: Long,
             apiMode: Boolean
            ) = {
    new WowPythonRunner(daemonCommand, workerCommand, execCommand, envVars, 64 * 1024, reuseWorker, recordLog, idleWorkerTimeoutMS)
  }

  def runner2(daemonCommand: Option[Seq[String]],
              workerCommand: Option[Seq[String]],
              execCommand: Array[Byte],
              envVars: util.Map[String, String],
              recordLog: String => Unit,
              apiMode: Boolean
             ) = {
    val idleWorkerTimeoutMS = if (apiMode) 60 * 60 * 24 * 1000 else 1000 * 60 * 2
    new WowPythonRunner(daemonCommand, workerCommand, execCommand, envVars, 64 * 1024, true, recordLog, idleWorkerTimeoutMS)
  }

  val PYSPARK_DAEMON_FILE_LOCATION = "/tmp/__mlsql__/python"
}

private[spark] object WowSpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
}
