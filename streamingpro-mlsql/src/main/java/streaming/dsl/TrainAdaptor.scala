package streaming.dsl

import java.util.UUID

import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/1/2018.
  */
class TrainAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {
    var tableName = ""
    var format = ""
    var path = ""
    var options = Map[String, String]()
    val owner = options.get("owner")
    var asTableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          tableName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = cleanStr(s.getText)
          path = evaluate(path)
        case s: ExpressionContext =>
          options += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case s: AsTableNameContext =>
          asTableName = cleanStr(s.tableName().getText)
        case _ =>
      }
    }
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlAlg = MLMapping.findAlg(format)
    //2.3.1
    val coreVersion = org.apache.spark.SPARK_VERSION.split("\\.").take(2).mkString(".") + ".x"
    if (sqlAlg.coreCompatibility.filter(f => f.coreVersion == coreVersion).size == 0) {
      throw new RuntimeException(s"name: $format class:${sqlAlg.getClass.getName} is not compatible with current core version:$coreVersion")
    }

    if (!sqlAlg.skipPathPrefix) {
      path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), path)
    }
    val newdf = if (options.getOrElse("runMode", "train") == "train") {
      sqlAlg.train(df, path, options)
    } else {
      sqlAlg.batchPredict(df, path, options)
    }

    val tempTable = if (asTableName.isEmpty) UUID.randomUUID().toString.replace("-", "") else asTableName
    newdf.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }
}

object MLMapping {
  val mapping = Map[String, String](
    "Word2vec" -> "streaming.dsl.mmlib.algs.SQLWord2Vec",
    "NaiveBayes" -> "streaming.dsl.mmlib.algs.SQLNaiveBayes",
    "RandomForest" -> "streaming.dsl.mmlib.algs.SQLRandomForest",
    "GBTRegressor" -> "streaming.dsl.mmlib.algs.SQLGBTRegressor",
    "LDA" -> "streaming.dsl.mmlib.algs.SQLLDA",
    "KMeans" -> "streaming.dsl.mmlib.algs.SQLKMeans",
    "FPGrowth" -> "streaming.dsl.mmlib.algs.SQLFPGrowth",
    "StringIndex" -> "streaming.dsl.mmlib.algs.SQLStringIndex",
    "GBTs" -> "streaming.dsl.mmlib.algs.SQLGBTs",
    "LSVM" -> "streaming.dsl.mmlib.algs.SQLLSVM",
    "HashTfIdf" -> "streaming.dsl.mmlib.algs.SQLHashTfIdf",
    "TfIdf" -> "streaming.dsl.mmlib.algs.SQLTfIdf",
    "LogisticRegressor" -> "streaming.dsl.mmlib.algs.SQLLogisticRegression",
    "RowMatrix" -> "streaming.dsl.mmlib.algs.SQLRowMatrix",
    "PageRank" -> "streaming.dsl.mmlib.algs.SQLPageRank",
    "StandardScaler" -> "streaming.dsl.mmlib.algs.SQLStandardScaler",
    "DicOrTableToArray" -> "streaming.dsl.mmlib.algs.SQLDicOrTableToArray",
    "TableToMap" -> "streaming.dsl.mmlib.algs.SQLTableToMap",
    "DL4J" -> "streaming.dsl.mmlib.algs.SQLDL4J",
    "TokenExtract" -> "streaming.dsl.mmlib.algs.SQLTokenExtract",
    "TokenAnalysis" -> "streaming.dsl.mmlib.algs.SQLTokenAnalysis",
    "TfIdfInPlace" -> "streaming.dsl.mmlib.algs.SQLTfIdfInPlace",
    "Word2VecInPlace" -> "streaming.dsl.mmlib.algs.SQLWord2VecInPlace",
    "RateSampler" -> "streaming.dsl.mmlib.algs.SQLRateSampler",
    "ScalerInPlace" -> "streaming.dsl.mmlib.algs.SQLScalerInPlace",
    "NormalizeInPlace" -> "streaming.dsl.mmlib.algs.SQLNormalizeInPlace",
    "PythonAlg" -> "streaming.dsl.mmlib.algs.SQLPythonAlg",
    "ConfusionMatrix" -> "streaming.dsl.mmlib.algs.SQLConfusionMatrix",
    "OpenCVImage" -> "streaming.dsl.mmlib.algs.processing.SQLOpenCVImage",
    "JavaImage" -> "streaming.dsl.mmlib.algs.processing.SQLJavaImage",
    "Discretizer" -> "streaming.dsl.mmlib.algs.SQLDiscretizer",
    "SendMessage" -> "streaming.dsl.mmlib.algs.SQLSendMessage",
    "JDBC" -> "streaming.dsl.mmlib.algs.SQLJDBC",
    "VecMapInPlace" -> "streaming.dsl.mmlib.algs.SQLVecMapInPlace",
    "DTFAlg" -> "streaming.dsl.mmlib.algs.SQLDTFAlg",
    "Map" -> "streaming.dsl.mmlib.algs.SQLMap",
    "PythonAlgBP" -> "streaming.dsl.mmlib.algs.SQLPythonAlgBatchPrediction",
    "ScalaScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "ScriptUDF" -> "streaming.dsl.mmlib.algs.ScriptUDF",
    "MapValues" -> "streaming.dsl.mmlib.algs.SQLMapValues",
    "ExternalPythonAlg" -> "streaming.dsl.mmlib.algs.SQLExternalPythonAlg",
    "BatchPythonAlg" -> "streaming.dsl.mmlib.algs.SQLBatchPythonAlg"
  )

  def findAlg(name: String) = {
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
        if (!name.contains(".") && (name.endsWith("InPlace") || name.endsWith("Ext"))) {
          Class.forName(s"streaming.dsl.mmlib.algs.SQL${name}").newInstance().asInstanceOf[SQLAlg]
        } else {
          throw new RuntimeException(s"${name} is not found")
        }
    }
  }
}
