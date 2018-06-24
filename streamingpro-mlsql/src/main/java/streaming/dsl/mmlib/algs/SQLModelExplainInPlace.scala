package streaming.dsl.mmlib.algs

import java.util

import org.apache.spark.ml.linalg.SQLDataTypes._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.util.ExternalCommandRunner
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.MetaConst.getDataPath

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Created by dxy_why on 2018/6/24.
 */
class SQLModelExplainInPlace extends SQLAlg with Functions {
  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val systemParam = mapParams("systemParam", params)
    val pythonPath = systemParam.getOrElse("pythonPath", "python")
    val paramMap = new util.HashMap[String, Object]()
    paramMap.put("systemParam", systemParam.asJava)
    paramMap.put("modelPath", params.getOrElse("modelPath", ""))
    val tfName = "mlsql_sk_attributes.py"
    val filePath = s"/python/${tfName}"
    val tfSource = Source.fromInputStream(ExternalCommandRunner.getClass.getResourceAsStream(filePath)).
      getLines().mkString("\n")
    val pythonScript = PythonScript(tfName, tfSource, filePath)
    val res = ExternalCommandRunner.run(Seq(pythonPath, pythonScript.fileName),
      paramMap,
      MapType(StringType, MapType(StringType, StringType)),
      pythonScript.fileContent,
      pythonScript.fileName, modelPath = null, kafkaParam = null,
      validateData = null
    )
    res.foreach(f => f)
    df.sparkSession.read.json("/tmp/attributes.json")
      .write.mode(SaveMode.Overwrite).parquet(getDataPath(path))

  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    UserDefinedFunction(null, VectorType, Some(Seq(VectorType)))
  }
}
