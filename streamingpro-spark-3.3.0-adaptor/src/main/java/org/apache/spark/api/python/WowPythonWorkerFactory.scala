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
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets
import java.util.Arrays

import scala.collection.mutable
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.{RedirectThread, Utils}

/**
  * Created by allwefantasy on 30/7/2018.
  */
class WowPythonWorkerFactory(daemonCommand: Option[Seq[String]],
                             workerCommand: Option[Seq[String]],
                             envVars: Map[String, String],
                             logCallback: (String) => Unit,
                             idleWorkerTimeoutMS: Long = WowPythonWorkerFactory.IDLE_WORKER_TIMEOUT_MS
                            )
  extends Logging {

  import WowPythonWorkerFactory._

  val flag = daemonCommand.get.mkString(" ")

  // Because forking processes from Java is expensive, we prefer to launch a single Python daemon
  // (pyspark/daemon.py) and tell it to fork new workers for our tasks. This daemon currently
  // only works on UNIX-based systems now because it uses signals for child management, so we can
  // also fall back to launching workers (pyspark/worker.py) directly.
  val useDaemon = !System.getProperty("os.name").startsWith("Windows")

  private val authHelper = new SocketAuthHelper(SparkEnv.get.conf)

  var daemon: Process = null
  val daemonHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  var daemonPort: Int = 0
  val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  val idleWorkers = new mutable.Queue[Socket]()
  var lastActivity = 0L
  new WowMonitorThread().start()

  var simpleWorkers = new mutable.WeakHashMap[Socket, Process]()

  val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  def create(): Socket = {
    if (useDaemon) {
      synchronized {
        if (idleWorkers.size > 0) {
          return idleWorkers.dequeue()
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker()
    }
  }

  /**
    * Connect to a worker launched through pyspark/daemon.py, which forks python processes itself
    * to avoid the high cost of forking from Java. This currently only works on UNIX-based systems.
    */
  private def createThroughDaemon(): Socket = {

    def createSocket(): Socket = {
      val socket = new Socket(daemonHost, daemonPort)
      val pid = new DataInputStream(socket.getInputStream).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }
      daemonWorkers.put(socket, pid)
      socket
    }

    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }

  /**
    * Launch a worker by executing worker.py directly and telling it to connect to us.
    */
  private def createSimpleWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // Create and start the worker
      //Arrays.asList(pythonExec, "-m", "pyspark.worker")
      val pb = new ProcessBuilder(workerCommand.get.asJava)
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream, logCallback)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream, StandardCharsets.UTF_8)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        simpleWorkers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  private def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        //Arrays.asList(pythonExec, "-m", "pyspark.daemon")
        val pb = new ProcessBuilder(daemonCommand.get.asJava)
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        daemonPort = in.readInt()

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream, logCallback)

      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage =
              s"""
                 |Error from python worker:
                 |  $formattedStderr
                 |PYTHONPATH was:
                 |  $pythonPath
                 |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /**
    * Redirect the given streams to our stderr in separate threads.
    */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream, logCallback: (String) => Unit) {
    try {
      new WowRedirectThread(stdout, "stdout reader for " + flag, System.err, logCallback).start()
      new WowRedirectThread(stderr, "stderr reader for " + flag, System.err, logCallback).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
    * Monitor all the idle workers, kill them after timeout.
    */
  private class WowMonitorThread extends Thread(s"Idle Worker Monitor for $flag") {

    setDaemon(true)

    override def run() {
      while (true) {
        synchronized {
          if (lastActivity + idleWorkerTimeoutMS < System.currentTimeMillis()) {
            cleanupIdleWorkers()
            lastActivity = System.currentTimeMillis()
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  private def cleanupIdleWorkers() {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        // the worker will exit after closing the socket
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  private def stopDaemon() {
    synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
      } else {
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  def stop() {
    stopDaemon()
  }

  def stopWorker(worker: Socket) {
    synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { pid =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(pid)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.close()
  }

  def releaseWorker(worker: Socket) {
    if (useDaemon) {
      synchronized {
        lastActivity = System.currentTimeMillis()
        idleWorkers.enqueue(worker)
      }
    } else {
      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  private class WowRedirectThread(
                                   in: InputStream,
                                   name: String,
                                   out: OutputStream,
                                   logCallback: (String) => Unit = (msg: String) => {},
                                   propagateEof: Boolean = false)
    extends Thread(name) {

    setDaemon(true)

    override def run() {
      {
        scala.util.control.Exception.ignoring(classOf[IOException]) {
          Utils.tryWithSafeFinally {
            val br = new BufferedReader(new InputStreamReader(in))
            var line = br.readLine()
            while (line != null) {
              logCallback(line)
              line = br.readLine()
            }
          } {
            if (propagateEof) {
              out.close()
            }
          }
        }
      }
    }
  }

}

object WowPythonWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_MS = 60000 * 60 * 24 // kill idle workers after 24 hours
}
