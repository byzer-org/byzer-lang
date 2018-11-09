package org.apache.spark

import java.net.Socket


/**
  * Created by allwefantasy on 30/7/2018.
  */
class MLSQLPythonEnv(env: SparkEnv, deployAPI: Boolean) {

  def sparkEnv = env

  def createPythonWorker(daemonCommand: Option[Seq[String]],
                         workerCommand: Option[Seq[String]],
                         envVars: Map[String, String],
                         logCallback: (String) => Unit,
                         idleWorkerTimeoutMS: Long,
                         noCache: Boolean = true
                        ): java.net.Socket = {
    APIDeployPythonRunnerEnv.createPythonWorker(daemonCommand, workerCommand, envVars, logCallback, idleWorkerTimeoutMS, noCache)
  }


  def destroyPythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.destroyPythonWorker(daemonCommand, workerCommand, envVars, worker)
  }


  def releasePythonWorker(daemonCommand: Option[Seq[String]],
                          workerCommand: Option[Seq[String]],
                          envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.releasePythonWorker(daemonCommand, workerCommand, envVars, worker)
  }
}
