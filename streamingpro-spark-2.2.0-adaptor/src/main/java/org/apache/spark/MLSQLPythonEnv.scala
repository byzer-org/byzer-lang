package org.apache.spark

import java.net.Socket



/**
  * Created by allwefantasy on 30/7/2018.
  */
class MLSQLPythonEnv(env: SparkEnv, deployAPI: Boolean) {

  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    if (deployAPI) {
      APIDeployPythonRunnerEnv.createPythonWorker(pythonExec, envVars)
    } else {
      env.createPythonWorker(pythonExec, envVars)
    }
  }


  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    if (deployAPI) {
      APIDeployPythonRunnerEnv.destroyPythonWorker(pythonExec, envVars, worker)
    } else {
      env.destroyPythonWorker(pythonExec, envVars, worker)
    }
  }


  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    if (deployAPI) {
      APIDeployPythonRunnerEnv.releasePythonWorker(pythonExec, envVars, worker)
    } else {
      env.releasePythonWorker(pythonExec, envVars, worker)
    }
  }
}
