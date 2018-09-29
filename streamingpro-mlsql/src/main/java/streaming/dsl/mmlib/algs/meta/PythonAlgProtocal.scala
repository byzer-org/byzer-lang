package streaming.dsl.mmlib.algs.meta

/**
  * Created by allwefantasy on 29/9/2018.
  */
case class PythonScript(fileName: String, fileContent: String, filePath: String, scriptType: PythonScriptType = Script)

sealed class PythonScriptType

case object Script extends PythonScriptType

case object MLFlow extends PythonScriptType

case object NormalProject extends PythonScriptType
