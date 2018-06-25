package streaming.dsl

import org.apache.spark.sql.SparkSession
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
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          tableName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = cleanStr(s.getText)
          path = evaluate(path)
          path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), path)
        case s: ExpressionContext =>
          options += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlAlg = MLMapping.findAlg(format)
    sqlAlg.train(df, path, options)
    scriptSQLExecListener.setLastSelectTable(null)
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
    "TensorFlow" -> "streaming.dsl.mmlib.algs.SQLTensorFlow",
    "StandardScaler" -> "streaming.dsl.mmlib.algs.SQLStandardScaler",
    "SKLearn" -> "streaming.dsl.mmlib.algs.SQLSKLearn",
    "DicOrTableToArray" -> "streaming.dsl.mmlib.algs.SQLDicOrTableToArray",
    "TableToMap" -> "streaming.dsl.mmlib.algs.SQLTableToMap",
    "DL4J" -> "streaming.dsl.mmlib.algs.SQLDL4J",
    "TokenExtract" -> "streaming.dsl.mmlib.algs.SQLTokenExtract",
    "TokenAnalysis" -> "streaming.dsl.mmlib.algs.SQLTokenAnalysis",
    "TfIdfInPlace" -> "streaming.dsl.mmlib.algs.SQLTfIdfInPlace",
    "Word2VecInPlace" -> "streaming.dsl.mmlib.algs.SQLWord2VecInPlace",
    "AutoFeature" -> "streaming.dsl.mmlib.algs.SQLAutoFeature",
    "RateSampler" -> "streaming.dsl.mmlib.algs.SQLRateSampler",
    "ScalerInPlace" -> "streaming.dsl.mmlib.algs.SQLScalerInPlace",
    "NormalizeInPlace" -> "streaming.dsl.mmlib.algs.SQLNormalizeInPlace",
    "PythonAlg" -> "streaming.dsl.mmlib.algs.SQLPythonAlg",
    "ConfusionMatrix" -> "streaming.dsl.mmlib.algs.SQLConfusionMatrix",
    "OpenCVImage" -> "streaming.dsl.mmlib.algs.processing.SQLOpenCVImage",
    "JavaImage" -> "streaming.dsl.mmlib.algs.processing.SQLJavaImage",
    "Discretizer" -> "streaming.dsl.mmlib.algs.SQLDiscretizer",
    "VecMapInPlace" -> "streaming.dsl.mmlib.algs.SQLVecMapInPlace"
  )

  def findAlg(name: String) = {
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
        if (!name.contains(".") && name.endsWith("InPlace")) {
          Class.forName(s"streaming.dsl.mmlib.algs.SQL${name}").newInstance().asInstanceOf[SQLAlg]
        } else {
          throw new RuntimeException(s"${name} is not found")
        }
    }
  }
}
