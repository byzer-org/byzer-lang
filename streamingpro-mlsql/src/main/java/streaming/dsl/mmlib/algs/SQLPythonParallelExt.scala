package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.dsl.mmlib.algs.python.{AutoCreateMLproject, PythonTrain}


/**
  * 2019-01-08 WilliamZhu(allwefantasy@gmail.com)
  */
class SQLPythonParallelExt(override val uid: String) extends SQLAlg with Functions with WowParams {
  def this() = this(BaseParams.randomUID())

  private def validateParams(params: Map[String, String]) = {
    params.get(feedMode.name).map(item => set(feedMode, item))


    params.get(scripts.name).map { item =>
      set(scripts, item)
      item
    }.getOrElse {
      if (!params.contains("pythonScriptPath") && !params.contains("pythonDescPath")) {
        throw new MLSQLException(s"${scripts.name} is required")
      }
    }

    params.get(entryPoint.name).map { item =>
      set(entryPoint, item)
      item
    }.getOrElse {

    }

    params.get(condaFile.name).map { item =>
      set(condaFile, item)
      item
    }.getOrElse {
    }
  }


  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    pythonCheckRequirements(df)
    val mlsqlContext = ScriptSQLExec.contextGetOrForTest()

    validateParams(params)

    val autoCreateMLproject = new AutoCreateMLproject($(scripts), $(condaFile), $(entryPoint))

    val projectPath = autoCreateMLproject.saveProject(df.sparkSession, path)

    var newParams = params

    newParams += ("enableDataLocal" -> ($(feedMode) == "file").toString)
    newParams += ("pythonScriptPath" -> projectPath)
    newParams += ("pythonDescPath" -> projectPath)

    val pt = new PythonTrain()
    pt.train_per_partition(df, path, newParams)

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = throw new MLSQLException("register is not support")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = throw new MLSQLException("register is not support")

  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    train(df, path, params)
  }

  final val feedMode: Param[String] = new Param(this, "feedMode",
    "file/iterator")
  setDefault(feedMode, "file")

  final val scripts: Param[String] = new Param(this, "scripts",
    "")

  final val projectPath: Param[String] = new Param(this, "projectPath",
    "")

  final val entryPoint: Param[String] = new Param(this, "entryPoint",
    "")

  final val condaFile: Param[String] = new Param(this, "condaFile",
    "")

}
