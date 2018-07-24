package org.apache.spark

import org.apache.spark.api.python.PythonWorkerFactory

/**
  * Created by allwefantasy on 24/7/2018.
  */
//object SparkEnvImplicit {
//  implicit def createPythonWorker(sparkEnv: SparkEnv): java.net.Socket = {
//
//    synchronized {
//      val key = (pythonExec, envVars)
//
//      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
//    }
//  }
//}
