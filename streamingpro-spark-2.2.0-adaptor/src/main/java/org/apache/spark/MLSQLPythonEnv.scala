package org.apache.spark

import java.net.Socket


/**
  * Created by allwefantasy on 30/7/2018.
  */
class MLSQLPythonEnv(env: SparkEnv, deployAPI: Boolean) {

  def createPythonWorker(pythonExec: String, envVars: Map[String, String], logCallback: (String) => Unit): java.net.Socket = {
    APIDeployPythonRunnerEnv.createPythonWorker(pythonExec, envVars, logCallback)
  }


  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.destroyPythonWorker(pythonExec, envVars, worker)
  }

  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    APIDeployPythonRunnerEnv.releasePythonWorker(pythonExec, envVars, worker)
  }
}
