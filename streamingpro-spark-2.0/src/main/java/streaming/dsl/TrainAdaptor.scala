package streaming.dsl

import org.apache.spark.sql.SparkSession
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 12/1/2018.
  */
class TrainAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var tableName = ""
    var format = ""
    var path = ""
    var options = Map[String, String]()
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: TableNameContext =>
          tableName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = withPathPrefix(scriptSQLExecListener.pathPrefix, cleanStr(s.getText))
        case s: ExpressionContext =>
          options += (cleanStr(s.identifier().getText) -> cleanStr(s.STRING().getText))
        case s: BooleanExpressionContext =>
          options += (cleanStr(s.expression().identifier().getText) -> cleanStr(s.expression().STRING().getText))
        case _ =>
      }
    }
    val df = scriptSQLExecListener.sparkSession.table(tableName)
    val sqlAlg = MLMapping.findAlg(format)
    sqlAlg.train(df, path, options)
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
    "TensorFlow" -> "streaming.dsl.mmlib.algs.SQLTensorFlow"
  )

  def registerMLFunctions(sparkSession: SparkSession) = {
    //collection operate
    sparkSession.udf.register("map", (col: String, reg: String) => {

    })
  }

  def findAlg(name: String) = {
    mapping.get(name.capitalize) match {
      case Some(clzz) =>
        Class.forName(clzz).newInstance().asInstanceOf[SQLAlg]
      case None =>
        throw new RuntimeException(s"${name} is not found")
    }
  }
}
