package streaming.dsl

import java.util.UUID

import streaming.dsl.parser.DSLSQLParser._
import streaming.dsl.template.TemplateMerge

/**
  * Created by allwefantasy on 12/1/2018.
  */
class RegisterAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {
    var functionName = ""
    var format = ""
    var path = ""
    var option = Map[String, String]()
    val resourceOwner = option.get("owner")
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: FunctionNameContext =>
          functionName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = TemplateMerge.merge(cleanStr(s.getText), scriptSQLExecListener.env().toMap)
        case s: ExpressionContext =>
          option += (cleanStr(s.qualifiedName().getText) -> evaluate(getStrOrBlockStr(s)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().qualifiedName().getText) -> evaluate(getStrOrBlockStr(s.expression())))
        case _ =>
      }
    }
    val alg = MLMapping.findAlg(format)
    if (!alg.skipPathPrefix) {
      path = resourceRealPath(scriptSQLExecListener, resourceOwner, path)
    }
    val sparkSession = scriptSQLExecListener.sparkSession
    val model = alg.load(sparkSession, path, option)
    val udf = alg.predict(sparkSession, model, functionName, option)
    if (udf != null) {
      scriptSQLExecListener.sparkSession.udf.register(functionName, udf)
    }
    val newdf = alg.explainModel(sparkSession, path, option)
    val tempTable = UUID.randomUUID().toString.replace("-", "")
    newdf.createOrReplaceTempView(tempTable)
    scriptSQLExecListener.setLastSelectTable(tempTable)
  }
}


