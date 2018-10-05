package org.apache.spark

import java.net.Socket
import java.util.Properties

import org.apache.spark.api.python.WowPythonWorkerFactory

import scala.collection.mutable

/**
  * Created by allwefantasy on 30/7/2018.
  */
object APIDeployPythonRunnerEnv {
  private val pythonWorkers = mutable.HashMap[(Seq[String], Map[String, String]), WowPythonWorkerFactory]()

  def workerSize = {
    pythonWorkers.size
  }

  def createPythonWorker(daemonCommand: Option[Seq[String]],
                         workerCommand: Option[Seq[String]],
                         envVars: Map[String, String],
                         logCallback: (String) => Unit,
                         idleWorkerTimeoutMS: Long): java.net.Socket = {
    synchronized {
      val key = (daemonCommand.get, envVars)
      pythonWorkers.getOrElseUpdate(key, new WowPythonWorkerFactory(
        daemonCommand,
        workerCommand,
        envVars,
        logCallback,
        idleWorkerTimeoutMS)).create()
    }
  }


  def destroyPythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String],
                          worker: Socket) {
    synchronized {
      val key = (daemonCommand.get, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }


  def releasePythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (daemonCommand.get, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }

  def setTaskContext(context: TaskContext) = {
    TaskContext.setTaskContext(context)
  }

  def createTaskContext() = {
    new TaskContextImpl(0, 0, 0, -1024, 0, null, new Properties, null)
  }
}
