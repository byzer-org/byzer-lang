package streaming.dsl.mmlib.algs.python

/**
  * Created by allwefantasy on 30/9/2018.
  */
class PythonAlgExecCommand(pythonScript: PythonScript,
                           mlflowConfig: Option[MLFlowConfig],
                           pythonConfig: Option[PythonConfig]) {

  def generateCommand(commandType: String) = {
    pythonScript.scriptType match {
      case MLFlow =>
        val project = MLProject.loadProject(pythonScript.filePath)
        Seq("bash", "-c", project.entryPointCommandWithConda(commandType))

      case _ =>
        Seq(pythonConfig.map(_.pythonPath).getOrElse(
          throw new IllegalArgumentException("pythonPath should be configured"))) ++
          pythonConfig.map(_.pythonParam).getOrElse(Seq()) ++
          Seq(pythonScript.fileName)
    }
  }


}





