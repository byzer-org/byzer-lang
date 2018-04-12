package streaming.dsl

import streaming.dsl.parser.DSLSQLParser._

/**
  * Created by allwefantasy on 12/1/2018.
  */
class RegisterAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  override def parse(ctx: SqlContext): Unit = {
    var functionName = ""
    var format = ""
    var path = ""
//    val owner = option.get("owner")
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>

      ctx.getChild(tokenIndex) match {
        case s: FunctionNameContext =>
          functionName = s.getText
        case s: FormatContext =>
          format = s.getText
        case s: PathContext =>
          path = withPathPrefix(scriptSQLExecListener.pathPrefix(None), cleanStr(s.getText))
        case _ =>
      }
    }
    val alg = MLMapping.findAlg(format)
    val sparkSession = scriptSQLExecListener.sparkSession
    val model = alg.load(sparkSession, path)
    val udf = alg.predict(sparkSession, model, functionName)
    scriptSQLExecListener.sparkSession.udf.register(functionName, udf)
  }
}


