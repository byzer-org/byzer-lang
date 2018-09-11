package streaming.dsl

import org.apache.spark.sql.SparkSession
import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/1/2018.
  */
class IncludeAdaptor(scriptSQLExecListener: ScriptSQLExecListener, preProcessListener: PreProcessIncludeListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {
    var functionName = ""
    var format = ""
    var path = ""
    var option = Map[String, String]()
    //    val owner = option.get("owner")
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: FunctionNameContext =>
          functionName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = TemplateMerge.merge(cleanStr(s.getText), scriptSQLExecListener.env().toMap)
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }
    val alg = IncludeAdaptor.findAlg(format, option)
    if (!alg.skipPathPrefix) {
      path = withPathPrefix(scriptSQLExecListener.pathPrefix(None), path)
    }

    val content = alg.fetchSource(scriptSQLExecListener.sparkSession, path, Map("format" -> format) ++ option)
    val originIncludeText = currentText(ctx)
    preProcessListener.addInclude(originIncludeText, content)
  }
}

object IncludeAdaptor {
  val mapping = Map[String, String](
    "hdfs" -> "streaming.dsl.mmlib.algs.includes.HDFSIncludeSource",
    "http" -> "streaming.dsl.mmlib.algs.includes.HTTPIncludeSource",

    "function" -> "streaming.dsl.mmlib.algs.includes.analyst.HttpBaseDirIncludeSource",
    "view" -> "streaming.dsl.mmlib.algs.includes.analyst.HttpBaseDirIncludeSource",
    "table" -> "streaming.dsl.mmlib.algs.includes.analyst.HttpBaseDirIncludeSource",
    "job" -> "streaming.dsl.mmlib.algs.includes.analyst.HttpBaseDirIncludeSource"
  )

  def findAlg(format: String, options: Map[String, String]) = {
    val clzz = mapping.getOrElse(format, options.getOrElse("implClass", "streaming.dsl.mmlib.algs.includes." + format))
    Class.forName(clzz).newInstance().asInstanceOf[IncludeSource]
  }

}

