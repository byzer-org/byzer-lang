package streaming.dsl.mmlib.algs.python


/**
  * Created by allwefantasy on 30/9/2018.
  */
class PythonAlgExecCommand(pythonScript: PythonScript,
                           mlflowConfig: Option[MLFlowConfig],
                           pythonConfig: Option[PythonConfig]) {

  def generateCommand = {
    pythonScript.scriptType match {
      case MLFlow =>
        val mm = Seq(mlflowConfig.map(_.mlflowPath).getOrElse(
          throw new IllegalArgumentException("mlflowPath should be configured")),
          mlflowConfig.map(_.mlflowCommand).getOrElse(
            throw new IllegalArgumentException("mlflowPath should be configured")), pythonScript.fileContent)

        mm ++ mlflowConfig.map(_.mlflowParam).getOrElse(
          Seq())

      case _ =>
        Seq(pythonConfig.map(_.pythonPath).getOrElse(
          throw new IllegalArgumentException("pythonPath should be configured"))) ++
          pythonConfig.map(_.pythonParam).getOrElse(Seq()) ++
          Seq(pythonScript.fileName)
    }
  }
}


